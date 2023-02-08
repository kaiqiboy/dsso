/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

// import java.io.{BufferedReader, BufferedWriter, File, InputStreamReader, OutputStreamWriter, PrintStream, PrintWriter}

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.lang.System.nanoTime
import java.net.{InetAddress, Socket}
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.util.control.Breaks

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.{InternalRow, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.catalyst.expressions.codegen.ByteCodeStats
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, Command, CommandResult, CreateTableAsSelect, CTERelationDef, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, ReplaceTableAsSelect, ReturnAnswer}
import org.apache.spark.sql.catalyst.rules.{PlanChangeLogger, Rule}
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.adaptive.{AdaptiveExecutionContext, InsertAdaptiveSparkPlan}
import org.apache.spark.sql.execution.bucketing.{CoalesceBucketsInJoin, DisableUnnecessaryBucketedScan}
import org.apache.spark.sql.execution.dynamicpruning.PlanDynamicPruningFilters
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.execution.reuse.ReuseExchangeAndSubquery
import org.apache.spark.sql.execution.streaming.{IncrementalExecution, OffsetSeqMetadata}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{CEO, CEO_LENGTH_THRESHOLD, CEO_PRUNE_AGGRESSIVE, CEO_SERVER_IP, CEO_SERVER_PORT}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.util.Utils


/**
 * The primary workflow for executing relational queries using Spark.  Designed to allow easy
 * access to the intermediate phases of query execution for developers.
 *
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
 */
class QueryExecution(
                      val sparkSession: SparkSession,
                      val logical: LogicalPlan,
                      val tracker: QueryPlanningTracker = new QueryPlanningTracker,
                      val mode: CommandExecutionMode.Value = CommandExecutionMode.ALL) extends Logging {

  val id: Long = QueryExecution.nextExecutionId

  // TODO: Move the planner an optimizer into here from SessionState.
  protected def planner = sparkSession.sessionState.planner

  var candidateSparkPlans: Option[Seq[SparkPlan]] = None

  def generateCandidatePlans: Unit = {
    candidateSparkPlans = Option(sparkSession.catalog.pruneSimilar match {
      case "similar" =>
        //        // scalastyle:off println
        //        println("candidate spark plans generated")
        //        // scalastyle:on println

        val plannerOut = planner.plan(ReturnAnswer(optimizedPlan))
        /*
        val concat = new PlanStringConcat()
        plannerOut.foreach { plan =>
          plan.treeString(concat.append, verbose = false, addSuffix = false, SQLConf.get.maxToStringFields, printOperatorId = true)
          concat.append("#&")
        }
        val planStructures = concat.toString.split("#&")
        QueryExecution.removeSimilarPlans(plannerOut, planStructures)
        */
        plannerOut
      case "none" => planner.plan(ReturnAnswer(optimizedPlan)).distinct

      case "agg" => planner.plan(ReturnAnswer(optimizedPlan)).distinct.filter { plan =>
        var nodeNames = new Array[String](0)
        plan.foreach { x =>
          if (x.nodeName.contains("Aggregate")) nodeNames = nodeNames :+ x.nodeName
        }
        nodeNames.sliding(2, 2).map(x => x(0) == x(1)).forall(_ == true)
      }

      case _ =>
        // scalastyle:off println
        println("Unknown prune strategy. Choose from 'agg', 'similar', and 'none'. Will not prune any plans")
        // scalastyle:on println
        planner.plan(ReturnAnswer(optimizedPlan)).distinct
    })
    sparkSession.catalog.candidatePlans = candidateSparkPlans
  }

  // The CTE map for the planner shared by the main query and all subqueries.
  private val cteMap = mutable.HashMap.empty[Long, CTERelationDef]

  def withCteMap[T](f: => T): T = {
    val old = QueryExecution.currentCteMap.get()
    QueryExecution.currentCteMap.set(cteMap)
    try f finally {
      QueryExecution.currentCteMap.set(old)
    }
  }

  def assertAnalyzed(): Unit = analyzed

  def assertSupported(): Unit = {
    if (sparkSession.sessionState.conf.isUnsupportedOperationCheckEnabled) {
      UnsupportedOperationChecker.checkForBatch(analyzed)
    }
  }

  lazy val analyzed: LogicalPlan = executePhase(QueryPlanningTracker.ANALYSIS) {
    // We can't clone `logical` here, which will reset the `_analyzed` flag.
    sparkSession.sessionState.analyzer.executeAndCheck(logical, tracker)
  }

  lazy val commandExecuted: LogicalPlan = mode match {
    case CommandExecutionMode.NON_ROOT => analyzed.mapChildren(eagerlyExecuteCommands)
    case CommandExecutionMode.ALL => eagerlyExecuteCommands(analyzed)
    case CommandExecutionMode.SKIP => analyzed
  }

  private def commandExecutionName(command: Command): String = command match {
    case _: CreateTableAsSelect => "create"
    case _: ReplaceTableAsSelect => "replace"
    case _: AppendData => "append"
    case _: OverwriteByExpression => "overwrite"
    case _: OverwritePartitionsDynamic => "overwritePartitions"
    case _ => "command"
  }

  private def eagerlyExecuteCommands(p: LogicalPlan) = p transformDown {
    case c: Command =>
      val qe = sparkSession.sessionState.executePlan(c, CommandExecutionMode.NON_ROOT)
      val result = SQLExecution.withNewExecutionId(qe, Some(commandExecutionName(c))) {
        qe.executedPlan.executeCollect()
      }
      CommandResult(
        qe.analyzed.output,
        qe.commandExecuted,
        qe.executedPlan,
        result)
    case other => other
  }

  lazy val withCachedData: LogicalPlan = sparkSession.withActive {
    assertAnalyzed()
    assertSupported()
    // clone the plan to avoid sharing the plan instance between different stages like analyzing,
    // optimizing and planning.
    sparkSession.sharedState.cacheManager.useCachedData(commandExecuted.clone())
  }

  def assertCommandExecuted(): Unit = commandExecuted

  lazy val optimizedPlan: LogicalPlan = {
    // We need to materialize the commandExecuted here because optimizedPlan is also tracked under
    // the optimizing phase
    assertCommandExecuted()
    executePhase(QueryPlanningTracker.OPTIMIZATION) {
      // clone the plan to avoid sharing the plan instance between different stages like analyzing,
      // optimizing and planning.
      val plan =
      sparkSession.sessionState.optimizer.executeAndTrack(withCachedData.clone(), tracker)
      // We do not want optimized plans to be re-analyzed as literals that have been constant
      // folded and such can cause issues during analysis. While `clone` should maintain the
      // `analyzed` state of the LogicalPlan, we set the plan as analyzed here as well out of
      // paranoia.
      plan.setAnalyzed()
      plan
    }
  }

  private def assertOptimized(): Unit = optimizedPlan

  //  lazy val candidatePlanString: String = explainString(ExplainMode.fromString("physicalplansformatted"))

  // get the plan before execution
  def selectedPlanString(): String = {
    val concat = new PlanStringConcat
    if (sparkSession.catalog.candidatePlans.isEmpty) generateCandidatePlans
    val candidatePlans = sparkSession.catalog.candidatePlans.get
    ExplainUtils.processPlan(candidatePlans(sparkSession.catalog.planIndex), concat.append)
    //    ExplainUtils.processPlan(candidateSparkPlans.get(sparkSession.catalog.planIndex), concat.append)
    concat.toString
  }

  lazy val sparkPlan: SparkPlan = withCteMap {
    // We need to materialize the optimizedPlan here because sparkPlan is also tracked under
    // the planning phase
    assertOptimized()
    executePhase(QueryPlanningTracker.PLANNING) {
      // Clone the logical plan here, in case the planner rules change the states of the logical
      // plan.
      val t = nanoTime()
      val a = QueryExecution.createSparkPlan(sparkSession, planner, optimizedPlan.clone())
      // scalastyle:off println
      println(s"==== plan exploration time a : ${(nanoTime() - t) * 1e-9} s.")
      // scalastyle:on println
      a
    }
  }

  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlan = withCteMap {
    // We need to materialize the optimizedPlan here, before tracking the planning phase, to ensure
    // that the optimization time is not counted as part of the planning phase.
    assertOptimized()
    executePhase(QueryPlanningTracker.PLANNING) {
      // clone the plan to avoid sharing the plan instance between different stages like analyzing,
      // optimizing and planning.
      QueryExecution.prepareForExecution(preparations, sparkPlan.clone())
    }
  }

  /**
   * Internal version of the RDD. Avoids copies and has no schema.
   * Note for callers: Spark may apply various optimization including reusing object: this means
   * the row is valid only for the iteration it is retrieved. You should avoid storing row and
   * accessing after iteration. (Calling `collect()` is one of known bad usage.)
   * If you want to store these rows into collection, please apply some converter or copy row
   * which produces new object per iteration.
   * Given QueryExecution is not a public class, end users are discouraged to use this: please
   * use `Dataset.rdd` instead where conversion will be applied.
   */
  lazy val toRdd: RDD[InternalRow] = new SQLExecutionRDD(
    executedPlan.execute(), sparkSession.sessionState.conf)

  /** Get the metrics observed during the execution of the query plan. */
  def observedMetrics: Map[String, Row] = CollectMetricsExec.collect(executedPlan)

  protected def preparations: Seq[Rule[SparkPlan]] = {
    QueryExecution.preparations(sparkSession,
      Option(InsertAdaptiveSparkPlan(AdaptiveExecutionContext(sparkSession, this))), false)
  }

  protected def executePhase[T](phase: String)(block: => T): T = sparkSession.withActive {
    tracker.measurePhase(phase)(block)
  }

  def simpleString: String = {
    val concat = new PlanStringConcat()
    simpleString(false, SQLConf.get.maxToStringFields, concat.append)
    withRedaction {
      concat.toString
    }
  }

  private def simpleString(
                            formatted: Boolean,
                            maxFields: Int,
                            append: String => Unit): Unit = {
    append("== Physical Plan ==\n")
    if (formatted) {
      try {
        ExplainUtils.processPlan(executedPlan, append)
      } catch {
        case e: AnalysisException => append(e.toString)
        case e: IllegalArgumentException => append(e.toString)
      }
    } else {
      QueryPlan.append(executedPlan,
        append, verbose = false, addSuffix = false, maxFields = maxFields)
    }
    append("\n")
  }

  def explainString(mode: ExplainMode): String = {
    val concat = new PlanStringConcat()
    explainString(mode, SQLConf.get.maxToStringFields, concat.append)
    withRedaction {
      concat.toString
    }
  }

  private def explainString(mode: ExplainMode, maxFields: Int, append: String => Unit): Unit = {
    val queryExecution = if (logical.isStreaming) {
      // This is used only by explaining `Dataset/DataFrame` created by `spark.readStream`, so the
      // output mode does not matter since there is no `Sink`.
      new IncrementalExecution(
        sparkSession, logical, OutputMode.Append(), "<unknown>",
        UUID.randomUUID, UUID.randomUUID, 0, OffsetSeqMetadata(0, 0))
    } else {
      this
    }

    mode match {
      case SimpleMode =>
        queryExecution.simpleString(false, maxFields, append)
      case ExtendedMode =>
        queryExecution.toString(maxFields, append)
      case CodegenMode =>
        try {
          org.apache.spark.sql.execution.debug.writeCodegen(append, queryExecution.executedPlan)
        } catch {
          case e: AnalysisException => append(e.toString)
        }
      case CostMode =>
        queryExecution.stringWithStats(maxFields, append)
      case FormattedMode =>
        queryExecution.simpleString(formatted = true, maxFields = maxFields, append)
      case PhysicalPlansMode =>
        this.candidateSparkPlans.get.zipWithIndex.foreach(p => append(s"=== Physical plan ${p._2}\n${p._1}\n"))
      case PhysicalPlansFormattedMode =>
        this.candidateSparkPlans.get.zipWithIndex.foreach(p => {
          append(s"=== Physical plan ${p._2}\n")
          ExplainUtils.processPlan(p._1, append)
        })
    }
  }

  private def writePlans(append: String => Unit, maxFields: Int): Unit = {
    val (verbose, addSuffix) = (true, false)
    append("== Parsed Logical Plan ==\n")
    QueryPlan.append(logical, append, verbose, addSuffix, maxFields)
    append("\n== Analyzed Logical Plan ==\n")
    try {
      if (analyzed.output.nonEmpty) {
        append(
          truncatedString(
            analyzed.output.map(o => s"${o.name}: ${o.dataType.simpleString}"), ", ", maxFields)
        )
        append("\n")
      }
      QueryPlan.append(analyzed, append, verbose, addSuffix, maxFields)
      append("\n== Optimized Logical Plan ==\n")
      QueryPlan.append(optimizedPlan, append, verbose, addSuffix, maxFields)
      append("\n== Physical Plan ==\n")
      QueryPlan.append(executedPlan, append, verbose, addSuffix, maxFields)
    } catch {
      case e: AnalysisException => append(e.toString)
    }
  }

  override def toString: String = withRedaction {
    val concat = new PlanStringConcat()
    toString(SQLConf.get.maxToStringFields, concat.append)
    withRedaction {
      concat.toString
    }
  }

  private def toString(maxFields: Int, append: String => Unit): Unit = {
    writePlans(append, maxFields)
  }

  def stringWithStats: String = {
    val concat = new PlanStringConcat()
    stringWithStats(SQLConf.get.maxToStringFields, concat.append)
    withRedaction {
      concat.toString
    }
  }

  private def stringWithStats(maxFields: Int, append: String => Unit): Unit = {
    val maxFields = SQLConf.get.maxToStringFields

    // trigger to compute stats for logical plans
    try {
      // This will trigger to compute stats for all the nodes in the plan, including subqueries,
      // if the stats doesn't exist in the statsCache and update the statsCache corresponding
      // to the node.
      optimizedPlan.collectWithSubqueries {
        case plan => plan.stats
      }
    } catch {
      case e: AnalysisException => append(e.toString + "\n")
    }
    // only show optimized logical plan and physical plan
    append("== Optimized Logical Plan ==\n")
    QueryPlan.append(optimizedPlan, append, verbose = true, addSuffix = true, maxFields)
    append("\n== Physical Plan ==\n")
    QueryPlan.append(executedPlan, append, verbose = true, addSuffix = false, maxFields)
    append("\n")
  }

  /**
   * Redact the sensitive information in the given string.
   */
  private def withRedaction(message: String): String = {
    Utils.redact(sparkSession.sessionState.conf.stringRedactionPattern, message)
  }

  /** A special namespace for commands that can be used to debug query execution. */
  // scalastyle:off
  object debug {
    // scalastyle:on

    /**
     * Prints to stdout all the generated code found in this plan (i.e. the output of each
     * WholeStageCodegen subtree).
     */
    def codegen(): Unit = {
      // scalastyle:off println
      println(org.apache.spark.sql.execution.debug.codegenString(executedPlan))
      // scalastyle:on println
    }

    /**
     * Get WholeStageCodegenExec subtrees and the codegen in a query plan
     *
     * @return Sequence of WholeStageCodegen subtrees and corresponding codegen
     */
    def codegenToSeq(): Seq[(String, String, ByteCodeStats)] = {
      org.apache.spark.sql.execution.debug.codegenStringSeq(executedPlan)
    }

    /**
     * Dumps debug information about query execution into the specified file.
     *
     * @param path        path of the file the debug info is written to.
     * @param maxFields   maximum number of fields converted to string representation.
     * @param explainMode the explain mode to be used to generate the string
     *                    representation of the plan.
     */
    def toFile(
                path: String,
                maxFields: Int = Int.MaxValue,
                explainMode: Option[String] = None): Unit = {
      val filePath = new Path(path)
      val fs = filePath.getFileSystem(sparkSession.sessionState.newHadoopConf())
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(filePath)))
      try {
        val mode = explainMode.map(ExplainMode.fromString(_)).getOrElse(ExtendedMode)
        explainString(mode, maxFields, writer.write)
        if (mode != CodegenMode) {
          writer.write("\n== Whole Stage Codegen ==\n")
          org.apache.spark.sql.execution.debug.writeCodegen(writer.write, executedPlan)
        }
        log.info(s"Debug information was written at: $filePath")
      } finally {
        writer.close()
      }
    }
  }
}

/**
 * SPARK-35378: Commands should be executed eagerly so that something like `sql("INSERT ...")`
 * can trigger the table insertion immediately without a `.collect()`. To avoid end-less recursion
 * we should use `NON_ROOT` when recursively executing commands. Note that we can't execute
 * a query plan with leaf command nodes, because many commands return `GenericInternalRow`
 * and can't be put in a query plan directly, otherwise the query engine may cast
 * `GenericInternalRow` to `UnsafeRow` and fail. When running EXPLAIN, or commands inside other
 * command, we should use `SKIP` to not eagerly trigger the command execution.
 */
object CommandExecutionMode extends Enumeration {
  val SKIP, NON_ROOT, ALL = Value
}

object QueryExecution {
  private val _nextExecutionId = new AtomicLong(0)

  private def nextExecutionId: Long = _nextExecutionId.getAndIncrement

  /**
   * Construct a sequence of rules that are used to prepare a planned [[SparkPlan]] for execution.
   * These rules will make sure subqueries are planned, make use the data partitioning and ordering
   * are correct, insert whole stage code gen, and try to reduce the work done by reusing exchanges
   * and subqueries.
   */
  private[execution] def preparations(
                                       sparkSession: SparkSession,
                                       adaptiveExecutionRule: Option[InsertAdaptiveSparkPlan] = None,
                                       subquery: Boolean): Seq[Rule[SparkPlan]] = {
    // `AdaptiveSparkPlanExec` is a leaf node. If inserted, all the following rules will be no-op
    // as the original plan is hidden behind `AdaptiveSparkPlanExec`.
    adaptiveExecutionRule.toSeq ++
      Seq(
        CoalesceBucketsInJoin,
        PlanDynamicPruningFilters(sparkSession),
        PlanSubqueries(sparkSession),
        RemoveRedundantProjects,
        EnsureRequirements(),
        // `RemoveRedundantSorts` needs to be added after `EnsureRequirements` to guarantee the same
        // number of partitions when instantiating PartitioningCollection.
        RemoveRedundantSorts,
        DisableUnnecessaryBucketedScan,
        ApplyColumnarRulesAndInsertTransitions(
          sparkSession.sessionState.columnarRules, outputsColumnar = false),
        CollapseCodegenStages()) ++
      (if (subquery) {
        Nil
      } else {
        Seq(ReuseExchangeAndSubquery)
      })
  }

  /**
   * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  private[execution] def prepareForExecution(
                                              preparations: Seq[Rule[SparkPlan]],
                                              plan: SparkPlan): SparkPlan = {
    val planChangeLogger = new PlanChangeLogger[SparkPlan]()
    val preparedPlan = preparations.foldLeft(plan) { case (sp, rule) =>
      val result = rule.apply(sp)
      planChangeLogger.logRule(rule.ruleName, sp, result)
      result
    }
    planChangeLogger.logBatch("Preparations", plan, preparedPlan)
    preparedPlan
  }

  /**
   * Transform a [[LogicalPlan]] into a [[SparkPlan]].
   *
   * Note that the returned physical plan still needs to be prepared for execution.
   */

  def createSparkPlan(
                       sparkSession: SparkSession,
                       planner: SparkPlanner,
                       plan: LogicalPlan): SparkPlan = {
    // TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
    //       but we will implement to choose the best plan.

    // analyze the optimized logical plan
    var numCandidates = 1
    val joinFactor = if (sparkSession.conf.get(CEO_PRUNE_AGGRESSIVE)) 2 else 3 // if prune aggressively, each join has 2 options, otherwise 3
    val aggFactor = if (sparkSession.catalog.pruneSimilar == "agg") 2 else 4
    plan.foreach { x =>
      if (x.nodeName.contains("Join")) numCandidates *= joinFactor
      else if (x.nodeName.contains("Aggregate")) {
        numCandidates *= aggFactor
        if (x.toString.split("\n").head.contains("DISTINCT")) numCandidates *= aggFactor
      }
    }

    // estimate the number of pruned plans (if both sides of a join are small, may underestimate the number by half)
    var smallColumnFactor = 1.0 // for division
    plan.foreach { x =>
      if (x.nodeName.contains("Relation")) {
        val columns = x.toString.split("Relation \\[")(1).split("\\]").head.split(",")
        if ((columns.toSet & sparkSession.catalog.smallColumns).nonEmpty) smallColumnFactor /= joinFactor
      }
    }
    numCandidates = (numCandidates * smallColumnFactor).toInt

    // scalastyle:off println
    println(s"Estimated number of plans: $numCandidates")
    // scalastyle:on println


    // fallback
    if (sparkSession.conf.get(CEO) && numCandidates > sparkSession.conf.get(CEO_LENGTH_THRESHOLD)) {
      // scalastyle:off println
      println(s"Number of plans to generate exceeds CEO_LENGTH_THRESHOLD ($numCandidates, " +
        s"${sparkSession.conf.get(CEO_LENGTH_THRESHOLD)}). " +
        s"Will stop exploration and fall back to original rule-based selection.")
      // scalastyle:on println
      sparkSession.conf.set("spark.sql.autoBroadcastJoinThreshold", "100m")
      return planner.planFallback(ReturnAnswer(plan)).head
    }

    /** first check if the plans has created by calling generateCandidatePlans and saved in catalog */
    val plans: Seq[SparkPlan] = sparkSession.catalog.candidatePlans.getOrElse {
      val plannerOut = planner.plan(ReturnAnswer(plan))
      if (plannerOut.length > 1) {
        sparkSession.catalog.pruneSimilar match {
          case "similar" =>
            val concat = new PlanStringConcat()
            plannerOut.foreach { plan =>
              plan.treeString(concat.append, verbose = false, addSuffix = false, SQLConf.get.maxToStringFields, printOperatorId = true)
              concat.append("#&")
            }
            val planStructures = concat.toString.split("#&")

            //    // scalastyle:off println
            //    println(s"plan enumeration time: ${NANOSECONDS.toMillis(System.nanoTime() - t).toString}")
            //    // scalastyle:on println
            QueryExecution.removeSimilarPlans(plannerOut, planStructures).distinct

          case "none" => plannerOut.distinct

          case "agg" => plannerOut.distinct.filter { plan =>
            var nodeNames = new Array[String](0)
            plan.foreach { x =>
              if (x.nodeName.contains("Aggregate")) nodeNames = nodeNames :+ x.nodeName
            }
            nodeNames.sliding(2, 2).map(x => x(0) == x(1)).forall(_ == true)
          }
          case _ =>
            // scalastyle:off println
            println("Unknown prune strategy. Choose from 'agg', 'similar', and 'none'. Will not prune any plans")
            // scalastyle:on println
            plannerOut.distinct
        }
      }
      else plannerOut
    }

    sparkSession.catalog.candidatePlans = None // reset to deal with cases when multiple plans are generated in one session

    val planIndex = if (sparkSession.conf.get(CEO) && plans.length > 1) {
      val concat = new PlanStringConcat()
      plans.foreach { plan =>
        ExplainUtils.processPlan(plan, concat.append)
        concat.append("splitMark")
      }
      val planStrings = concat.toString.split("splitMark")

      val t = nanoTime
      val idx = QueryExecution.predictCost(sparkSession, planStrings)
      // scalastyle:off println
      println(s"Predicting cost takes ${(nanoTime - t) * 1e-9}s.")
      println(s"Selected idx: $idx")
      println(planStrings(idx))
      // scalastyle:on println
      idx
    }
    else sparkSession.catalog.planIndex
    if (plans.length > planIndex) plans(planIndex) // for queries like create table, disable optimization
    else plans.head
  }

  /**
   * Prepare the [[SparkPlan]] for execution.
   */
  def prepareExecutedPlan(spark: SparkSession, plan: SparkPlan): SparkPlan = {
    prepareForExecution(preparations(spark, subquery = true), plan)
  }

  /**
   * Transform the subquery's [[LogicalPlan]] into a [[SparkPlan]] and prepare the resulting
   * [[SparkPlan]] for execution.
   */
  def prepareExecutedPlan(spark: SparkSession, plan: LogicalPlan): SparkPlan = {
    val t = nanoTime()
    val sparkPlan = createSparkPlan(spark, spark.sessionState.planner, plan.clone())
    // scalastyle:off println
    println(s"==== plan exploration time b: ${
      (nanoTime() - t) * 1e-9
    } s.")
    // scalastyle:on println
    prepareExecutedPlan(spark, sparkPlan)
  }

  private val currentCteMap = new ThreadLocal[mutable.HashMap[Long, CTERelationDef]]()

  def cteMap: mutable.HashMap[Long, CTERelationDef] = currentCteMap.get()

  // process strings for similarity check
  def processString(str: String): Seq[String] = {
    str.
      split("\n").
      map(x => x.replaceAll("[^A-Za-z]|(unknown)", "")).
      filterNot(x => List("Project", "Filter", "Scancsv", "Scanjson").contains(x))
  }

  def removeSimilarPlans(candidatePlans: Seq[SparkPlan], planStructures: Seq[String]): Seq[SparkPlan] = {
    //    // debug
    //    val t = System.nanoTime()

    // use reverse to keep the first plan of those with the same structure
    val zippedPlans = (planStructures.map(processString) zip candidatePlans.zipWithIndex).reverse.toMap
    val pruned = zippedPlans.values.toSeq.sortBy(_._2)
    val plans = pruned.map(_._1)

    //    // debug
    //    // scalastyle:off println
    //    println(s"remove similar plan time: ${NANOSECONDS.toMillis(System.nanoTime() - t).toString}")
    //    // scalastyle:on println
    plans
  }

  def genResourcesString(sparkSession: SparkSession): String = {
    val confMap = sparkSession.conf.getAll
    val n_core = confMap("spark.executor.cores").toInt
    val g_mem_raw = confMap("spark.executor.memory")
    val g_mem = if (g_mem_raw.endsWith("g") || g_mem_raw.endsWith("G")) g_mem_raw.dropRight(1)
    else if (g_mem_raw.endsWith("m") || g_mem_raw.endsWith("M")) (g_mem_raw.dropRight(1).toDouble / 1024).toString
    else throw new ArithmeticException(s"Unparsable g_memory: $g_mem_raw")
    val total_executors = confMap("spark.cores.max").toInt
    val n_executors = total_executors / n_core

    // assume one worker on one machine, get executor info and remove the driver on master
    val machines = sparkSession.sparkContext.getExecutorMemoryStatus.keys.toArray.map(_.split(":").head)
    val master = confMap("spark.master").split("//").last.split(":").head
    val n_worker = if (machines.count(_.contains(master)) > 1) machines.distinct.length
    else machines.distinct.length - 1
    (n_executors, g_mem, n_core, n_worker).toString
  }

  // the interface to call the deployed DNN-based cost estimation model and get the best plan ID
  def predictCost(sparkSession: SparkSession, processedPlans: Seq[String]): Int = {
    val resourcesString = genResourcesString(sparkSession)
    val candidatePlanString = processedPlans.zipWithIndex.map {
      case (x, y) =>
        s"=== Physical plan $y\n" + x
    }

    /** write plans to a file and send via socket */
    //    val ceoDir = sparkSession.conf.get(CEO_DIR)
    //    val costEstimationDir = if (ceoDir.last == '/') {
    //      ceoDir.dropRight(1)
    //    } else ceoDir
    //    val input = s"$costEstimationDir/input-${DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now)}"
    //    new PrintWriter(input) {
    //      write(resourcesString + "\n")
    //      write(candidatePlanString.mkString("\n"))
    //      close
    //    }
    //    var resString = ""
    //    val ip = sparkSession.conf.get(CEO_SERVER_IP)
    //    val port = sparkSession.conf.get(CEO_SERVER_PORT)
    //    val socket = new Socket(InetAddress.getByName(ip), port)
    //    val out = new PrintStream(socket.getOutputStream)
    //    val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
    //    out.print(input)
    //    val loop = new Breaks
    //    loop.breakable {
    //      while (true) {
    //        resString = in.readLine()
    //        if (in.readLine().contains("done")) loop.break
    //      }
    //    }
    //    in.close()
    //    out.close()
    //    socket.close()

    /** directly send text via socket */
    val ip = sparkSession.conf.get(CEO_SERVER_IP)
    val port = sparkSession.conf.get(CEO_SERVER_PORT)
    val socket = new Socket(InetAddress.getByName(ip), port)
    val out = new OutputStreamWriter(socket.getOutputStream, StandardCharsets.UTF_8)
    out.write(resourcesString + "\n")
    out.write(candidatePlanString.mkString("\n"))
    out.write("done")
    out.flush()
    var resString = ""
    val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
    val loop = new Breaks
    loop.breakable {
      while (true) {
        resString = in.readLine()
        if (in.readLine().contains("done")) loop.break
      }
    }
    in.close()
    out.close()
    socket.close()

    var best_idx = 0
    try {
      best_idx = resString.split("idx: ").last.toInt
      //      if (sparkSession.catalog.deleteTmpCeoFiles) new File(input).delete()
    } catch {
      case _: Exception =>
        // scalastyle:off println
        println("Failed to predict the best plan. Selecting the first one.")
      //        val ceoDir = sparkSession.conf.get(CEO_DIR)
      //        val costEstimationDir = if (ceoDir.last == '/') {
      //          ceoDir.dropRight(1)
      //        } else ceoDir
      //        val input = s"$costEstimationDir/input-${DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm").format(LocalDateTime.now)}"
      //        new PrintWriter(input) {
      //          write(resourcesString + "\n")
      //          write(candidatePlanString.mkString("\n"))
      //          close
      //        }
      // scalastyle:on println
    }
    best_idx
  }

  // the interface to call the deployed DNN-based cost estimation model and get the best plan ID
  //  def predictCostOld(sparkSession: SparkSession, processedPlans: Seq[String]): Int = {
  //    val resourcesString = genResourcesString(sparkSession)
  //    val costEstimationDir = if (sparkSession.catalog.costEstimationDir.nonEmpty &&
  //      sparkSession.catalog.costEstimationDir.last == '/') {
  //      sparkSession.catalog.costEstimationDir.dropRight(1)
  //    } else sparkSession.catalog.costEstimationDir
  //    assert(Files.exists(Paths.get(s"$costEstimationDir/inference.py")),
  //      s"The directory $costEstimationDir for cost estimation files does not exist.")
  //
  //    val candidatePlanString = processedPlans.zipWithIndex.map { case (x, y) =>
  //      s"=== Physical plan $y\n" + x
  //    }
  //
  //    // debug
  //    var t = System.nanoTime()
  //
  //    new PrintWriter(s"$costEstimationDir/input") {
  //      write(resourcesString + "\n")
  //      write(candidatePlanString.mkString("\n"))
  //      close
  //    }
  //    // debug
  //    // scalastyle:off println
  //    println(s"writing plan time: ${NANOSECONDS.toMillis(System.nanoTime() - t).toString} ms")
  //    // scalastyle:on println
  //
  //    // debug
  //    t = System.nanoTime()
  //
  //    val rt = Runtime.getRuntime()
  //    val command = s"python $costEstimationDir/inference.py -i input"
  //    val proc = rt.exec(command)
  //    val stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream)) // the output from python
  //    val t2 = nanoTime()
  //    val out = try {
  //      stdInput.lines().collect(Collectors.joining("\n"))
  //        .split("\n").filter(_.startsWith("best idx:")).head.split(": ").last
  //        .stripMargin.toInt
  //    } catch {
  //      case _: Exception =>
  //        // scalastyle:off println
  //        println("Failed to predict the best plan. Fall back to the first one.")
  //        // scalastyle:on println
  //        0
  //    }
  //    // debug
  //    // scalastyle:off println
  //    println(s"python time viewing from spark: ${NANOSECONDS.toMillis(System.nanoTime() - t).toString}, " +
  //      s"${NANOSECONDS.toMillis(System.nanoTime() - t2).toString} ms")
  //    // scalastyle:on println
  //    out
  //  }
}
