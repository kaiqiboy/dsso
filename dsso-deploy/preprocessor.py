# for plan vectorization
import torch
import pickle
import numpy as np
import json
import re
import pickle
from sklearn.preprocessing import normalize
import time
from fse.models import uSIF

class TreeNode:
    def __init__(self, value, idx):
        self.value = value 
        self.children = [] 
        self.idx = idx
    
    def add_child(self, child_node):
        self.children.append(child_node) 

    def __repr__(self, level=0):
        ret = "  "*level+repr(self.value)+"\n"
        for child in self.children:
            ret += child.__repr__(level+1)
        return ret

class Operation:
    def __init__(self, s):
        idx, operator = self.get_idx_operator(s)
        self.idx = idx
        self.operator = operator
        self.auxi = self.get_auxi(s)
    def __repr__(self):
        return "({}) {} \n  {}\n".format(self.idx, self.operator, self.auxi)
    def get_idx_operator(self, s):
        idx = int(s[0].split(")")[0][1:])
        operator = s[0].split(")")[1].split("[")[0].strip()
        return idx, operator
    def get_auxi(self, s):
        auxi_dict = {}
        for i in s[1:]:
            k = re.sub(r'[^a-zA-Z]', '', i.split(":")[0])
            v = ":".join(i.split(":")[1:]).strip() 
            v = re.sub(r'#[0-9]+', '', v.replace("[","").replace("]","")) # remove "#xx" and "[]"
            v = v.split(", ") # take the key words
            auxi_dict[k] = v
        return auxi_dict

class Condition: 
    def __init__(self, column, operator, operand):
        self.column = column
        self.operator = operator
        self.operand = operand
    def __repr__(self):
        return "Condition <{} {} {}>".format(self.column, self.operator, self.operand)
        
class Preprocessor():
    def __init__(self, column_onehot_dir, \
                table_onehot_dir,\
                operation_onehot_dir, \
                table_metadata_dir,\
                max_resource,\
                usif_model_dir,\
                max_seq_len,\
                max_column_len,\
                condition_dim):

        t = time.time()
        # load encoding variables
        self.column_onehot = pickle.load(open(column_onehot_dir, 'rb'))
        self.table_onehot = pickle.load(open(table_onehot_dir, 'rb'))
        self.operation_onehot = pickle.load(open(operation_onehot_dir, 'rb'))
        # self.usif_model = pickle.load(open(usif_model_dir, 'rb'))

        self.usif_model = uSIF.load(usif_model_dir)

        self.normalizes_column, self.normalizes_table = self.parse_table_metadata(table_metadata_dir)
        self.max_resource = [max_resource['n_executor'], max_resource['g_mem'], max_resource['n_core'], max_resource['n_worker']]
        self.max_seq_len = max_seq_len
        self.max_column_len = max_column_len
        self.normalized_resources = 0
        self.condition_dim = condition_dim

        self.table_column_dict = {}
        for i in self.normalizes_column.keys():
            self.table_column_dict[i.split(".")[1]] = i.split(".")[0]

    def parse_table_metadata(self, file_name="tpch30-metadata.json"):
        f = open(file_name)
        columns_all = []
        tables_all = []
        for line in f:
            metadata = json.loads(line)
            table_name = metadata['name']
            columns = metadata['columns']
            tables_all.append((table_name, metadata['length'], metadata['size']))
            for k in columns.keys():
                columns_all.append((table_name + '.' + k, columns[k]))

        # normalize columns
        numerical = []
        minValues = []
        maxValues = []
        nullss = []
        distincts = []
        for _,i in columns_all:
            numerical.append(i['numerical'])
            minValues.append(i['minValue'])
            maxValues.append(i['maxValue'])
            nullss.append(i['nulls'])
            distincts.append(i['distinct'])
        minValues = normalize([minValues], norm="max")
        maxValues = normalize([maxValues], norm="max")
        nullss = normalize([nullss], norm="max")
        distincts = normalize([distincts], norm="max")

        normalizes_column = {}
        for i, (k, _) in enumerate(columns_all):
            if(numerical[i] == 1):
                normalizes_column[k] = np.array((numerical[i], minValues[0][i], maxValues[0][i], nullss[0][i], distincts[0][i]))
            else:
                normalizes_column[k] = np.array((numerical[i], 0, 1, nullss[0][i], distincts[0][i]))
                
        normalized_table = normalize([x[1:] for x in tables_all], axis=0)
        normalizes_table = {}
        for i in range(len(normalized_table)):
            normalizes_table[tables_all[i][0]] = np.array((normalized_table[i][0],normalized_table[i][1]))
        return (normalizes_column, normalizes_table)


    def parse_table_metadata(self, file_name):
        f = open(file_name)
        columns_all = []
        tables_all = []
        for line in f:
            metadata = json.loads(line)
            table_name = metadata['name']
            columns = metadata['columns']
            tables_all.append((table_name, metadata['length'], metadata['size']))
            for k in columns.keys():
                columns_all.append((table_name + '.' + k, columns[k]))
        # normalize columns
        numerical = []
        minValues = []
        maxValues = []
        nullss = []
        distincts = []
        for _,i in columns_all:
            numerical.append(i['numerical'])
            minValues.append(i['minValue'])
            maxValues.append(i['maxValue'])
            nullss.append(i['nulls'])
            distincts.append(i['distinct'])
        minValues = normalize([minValues], norm="max")
        maxValues = normalize([maxValues], norm="max")
        nullss = normalize([nullss], norm="max")
        distincts = normalize([distincts], norm="max")
        normalizes_column = {}
        for i, (k, _) in enumerate(columns_all):
            if(numerical[i] == 1):
                normalizes_column[k] = np.array((numerical[i], minValues[0][i], maxValues[0][i], nullss[0][i], distincts[0][i]))
            else:
                normalizes_column[k] = np.array((numerical[i], 0, 1, nullss[0][i], distincts[0][i]))
        normalized_table = normalize([x[1:] for x in tables_all], axis=0)
        normalizes_table = {}
        for i in range(len(normalized_table)):
            normalizes_table[tables_all[i][0]] = np.array((normalized_table[i][0],normalized_table[i][1]))
        return (normalizes_column, normalizes_table)

    def normalize_resource(self, resource):
        resource = [resource['n_executor'], resource['g_mem'], resource['n_core'], resource['n_worker']]
        return np.array(resource) / np.array(self.max_resource)

    # helper functions, find step 1 below to start
    def split_plan(self, operations): # operations: list of string
        # split skeleton and details
        skeleton = []
        detail = []
        flag = False
        for o in operations:
            if(len(o) > 0):
                if(o[0] == "("):
                    flag = True
                if(flag == False):
                    skeleton.append(o)
                else:
                    detail.append(o)
        skeleton = skeleton[1:]
        
        return skeleton, detail

    # build a tree from the strings  
    def parse_skeleton(self, skeleton):
        nodes = []
        for (i, o) in enumerate(skeleton):
            # each line is a node, find its parent by back-tracking ":"
            level = 0
            for (j, l) in enumerate(o):
                if(l == "-"):
                    level = j
                    break
            # construct node
            name = re.sub(r"[:* +-]", '', o)
            idx = int(name.split("(")[-1][:-1]) - 1 # the index starts from 1, make it start from 0
            name = name.split("(")[0]
            node = TreeNode(name, idx)

            # find its parent
            if(o.strip(" :").startswith("+-")):
                x = i-1
                for x in range(i-1, 0, -1):
                    if(skeleton[x][level-1] != ":"):
                        break
                nodes[x].add_child(node)
                
            elif(o.strip(" :").startswith("-")):
                nodes[-1].add_child(node)

            # add node to nodes
            nodes.append(node)

        return nodes[0], nodes # the root of the tree and a list of all nodes

    # generate the structure matrix
    def gen_struct_matrix(self, nodes):
        nodes_len = len(nodes)
        matrix = np.zeros([nodes_len, nodes_len])
        for node in nodes:
            for child in node.children:
                matrix[node.idx, child.idx] = 1 # is parent
                matrix[child.idx, node.idx] = -1 # is child
        return matrix

    # parse details
    def parse_detail(self, detail):
        operations = [[detail[0]]]
        parsed_operations = []
        for line in detail[1:]:
            if(line.startswith("(")):
                operations.append([line])
            else:
                last = operations[-1]
                operations[:-1].append(last.append(line))
        for operation in operations:
            parsed_operations.append(Operation(operation))
        return parsed_operations # return a list of operations, each of which contains operator and auxiliary info

    def reformat_scanparquet(self,operation):
        auxi = operation.auxi
        table = auxi["Location"][0].split("/")[-1]
        columns = auxi["Output"]
        columns_w_table = [table+"."+c for c in columns]
        new_auxi = {}
        new_auxi["Table"] = [table]
        new_auxi["Columns"] = columns_w_table
        conditions = []
        if auxi.get("PushedFilters"):
            for i in auxi["PushedFilters"]:
                if len(i.split("(")) > 1:
                    operator = i.split("(")[0]
                    objects = i.split("(")[1][:-1].split(",")
                    o = table+"."+objects[0]
                    if(len(objects) > 1):
                        operand = objects[1]
                        if(operand.isnumeric()):
                            operand = float(operand)
                        elif(operand in columns):
                            operand = table+"."+operand
                        else:
                            pass
                    else: 
                        operand = ''
                    conditions.append(Condition(o, operator, operand))
            new_auxi["Condition"] = []
            for i in auxi["PushedFilters"]:
                new_auxi["Condition"] += i.split(", ")
        operation.auxi = new_auxi
        return operation

    def reformat_scancsv(self,operation):
        auxi = operation.auxi
        table = auxi["Location"][0].split("/")[-1].split(".")[0]
        columns = [i.replace("L","") for i in auxi["Output"]]
        columns_w_table = [table+"."+c for c in columns]
        new_auxi = {}
        new_auxi["Table"] = [table]
        new_auxi["Columns"] = columns_w_table
        conditions = []
        
        if auxi.get('PushedFilters'):
            # hard code for wrongly split
            pushedfilters = []
            i = 0
            while i < len(auxi["PushedFilters"]):
                e = auxi["PushedFilters"][i]
                if e.startswith("In"): 
                    pushedfilters.append(e+","+auxi["PushedFilters"][i+1])
                    i += 2
                else: 
                    pushedfilters.append(e)
                    i += 1
                    
            # print(pushedfilters)
            for i in pushedfilters:
                operator = i.split("(")[0]
                objects = i.split("(")[1][:-1].split(",")
                o = table+"."+objects[0]
                if(len(objects) > 1):
                    operand = objects[1]
                    if(operand.isnumeric()):
                        operand = float(operand)
                    elif(operand in columns):
                        operand = table+"."+operand
                    else:
                        pass
                else: 
                    operand = ''
                conditions.append(Condition(o, operator, operand))
            new_auxi["Condition"] = []
            for i in auxi["PushedFilters"]:
                new_auxi["Condition"] += i.split(", ")
        operation.auxi = new_auxi
        return operation

    def reformat_logicalrelation(self,operation, parents):
        auxi = operation.auxi
        columns = [i for i in auxi['Arguments'] if not i in ['parquet', 'true', 'false']]
        columns = [i[:-1] for i in columns if i.endswith("L")]
        tables = [self.table_column_dict[i] for i in columns]
        auxi['Table'] = list(dict.fromkeys(tables))
        auxi['Columns'] = [self.table_column_dict[i]+'.'+i for i in columns]
        return operation

    def reformat_filter(self,operation, parents):
        parent = parents[0]
        table = parent.auxi["Table"]
        auxi = operation.auxi
        auxi["Table"] = table
        if auxi.get('Input'):
            columns = [i.replace("L","") for i in auxi["Input"]]
            auxi['Columns'] = [table[0]+"."+i for i in columns]
        else:
            auxi['Columns'] = parent.auxi["Columns"]
        # todo parse conditions
        if auxi.get("Condition"):
            auxi['Condition'] = [i.replace("L", "") for i in auxi['Condition']]
        else:
            auxi['Condition'] = []
        return operation

    def reformat_project(self,operation, parents): 
        # project acts like glue - it records the tables and comlumns of the ancestors 
        tables = []
        columns = []
        for parent in parents:
            tables += parent.auxi["Table"]
            columns += parent.auxi["Columns"]
        auxi = operation.auxi
        auxi["Table"] = tables
        auxi['Columns'] = []
        output =  auxi["Output"] if auxi.get("Output") else auxi["Arguments"]
        for i in output:
            for j in columns:
                if i.replace("L", "") in j:
                    auxi['Columns'].append(j)
                    break
        return operation

    def reformat_exchange(self,operation, parents):
        parent = parents[0]
        table = parent.auxi["Table"]
        auxi = operation.auxi
        auxi["Table"] = table
        auxi['Columns'] = [table[0]+"."+i.strip('L') for i in auxi["Input"]]
        return operation

    def reformat_reusedexchange(self,operation, parents):
        auxi = operation.auxi
        columns = [i.strip('L') for i in auxi["Output"]]
        tables = [self.table_column_dict[i] for i in columns]
        auxi['Table'] = list(dict.fromkeys(tables))
        auxi['Columns'] = [self.table_column_dict[i]+'.'+i for i in columns]
        return operation

    def reformat_hashagg(self,operation, parents):
        tables = []
        columns = []
        for parent in parents:
            tables += parent.auxi["Table"]
            columns += parent.auxi["Columns"]
        auxi = operation.auxi
        auxi["Table"] = tables
        auxi['Columns'] = columns
        auxi['Condition'] = [i.replace("L", "") for i in [" AND ".join([i for i in \
            auxi['Functions'] + auxi['AggregateAttributes']])]]
        return operation    

    def reformat_sortagg(self,operation, parents):
        tables = []
        columns = []
        for parent in parents:
            tables += parent.auxi["Table"]
            columns += parent.auxi["Columns"]
        auxi = operation.auxi
        auxi["Table"] = tables
        auxi['Columns'] = columns
        auxi['Condition'] = [i.replace("L", "") for i in [" AND ".join([i for i in \
            auxi['Functions'] + auxi['AggregateAttributes']])]]
        return operation 

    def reformat_ohagg(self, operation, parents):
        tables = []
        columns = []
        for parent in parents:
            tables += parent.auxi["Table"]
            columns += parent.auxi["Columns"]
        auxi = operation.auxi
        auxi["Table"] = tables
        auxi['Columns'] = columns
        auxi['Condition'] = [i.replace("L", "") for i in [" AND ".join([i for i in \
            auxi['Functions']])]]
        return operation       

    def reformat_agg(self,operation, parents):
        tables = []
        columns = []
        for parent in parents:
            tables += parent.auxi["Table"]
            columns += parent.auxi["Columns"]
        auxi = operation.auxi
        auxi["Table"] = tables
        auxi['Columns'] = columns
        auxi['Condition'] = []
        return operation  

    def reformat_sort(self,operation, parents):
        # print(parents)
        parent = parents[0]
        table = parent.auxi["Table"]
        auxi = operation.auxi
        columns = [i.replace("L","") for i in auxi["Input"]]
        auxi["Table"] = table
        auxi['Columns'] = [table[0]+"."+i for i in columns]
        auxi["Condition"] = [" AND ".join(auxi['Arguments'])]
        return operation  

    def reformat_smjoin(self,operation, parents):
        tables = []
        columns = []
        for parent in parents:
            tables += parent.auxi["Table"]
            columns += parent.auxi["Columns"]
        auxi = operation.auxi
        auxi["Table"] = tables
        auxi['Columns'] = []
        for i in auxi["Leftkeys"] + auxi["Rightkeys"]:
            for j in columns:
                if i.replace("L","") in j:
                    auxi['Columns'].append(j)
        return operation

    def reformat_join(self,operation, parents):  
        tables = []
        columns = []
        for parent in parents:
            tables += parent.auxi["Table"]
            columns += parent.auxi["Columns"]
        auxi = operation.auxi
        auxi["Table"] = tables
        auxi['Columns'] = []
        c = [i.replace("L","").replace("(","").replace(")","") for i in auxi['Arguments'][1].split(" = ")] # todo: currently hardcoded
        for i in c:
            for j in columns:
                if i in j:
                    auxi['Columns'].append(j)
                    break
        return operation 
        
    def reformat_bchjoin(self,operation, parents):
        tables = []
        columns = []
        for parent in parents:
            tables += parent.auxi["Table"]
            columns += parent.auxi["Columns"]
        auxi = operation.auxi
        auxi["Table"] = tables
        auxi['Columns'] = []
        for i in auxi["Leftkeys"] + auxi["Rightkeys"]:
            i = i.replace("L", "")
            for j in columns:
                if i in j:
                    auxi['Columns'].append(j)
                    break
        return operation 

    def reformat_schjoin(self,operation, parents):
        tables = []
        columns = []
        for parent in parents:
            tables += parent.auxi["Table"]
            columns += parent.auxi["Columns"]
        auxi = operation.auxi
        auxi["Table"] = tables
        auxi['Columns'] = []
        for i in auxi["Leftkeys"] + auxi["Rightkeys"]:
            i = i.replace("L", "")
            for j in columns:
                if i in j:
                    auxi['Columns'].append(j)
                    break
        return operation 

    def reformat_cartesianproduct(self,operation, parents):
        tables = []
        columns = []
        for parent in parents:
            tables += parent.auxi["Table"]
            columns += parent.auxi["Columns"]
        auxi = operation.auxi
        auxi["Table"] = tables
        auxi['Columns'] = []
        c = [i.strip() for i in parent.auxi["Columns"][0].replace("L","").replace("(","").replace(")","").split("=")]
        for i in c:
            for j in columns:
                if i in j:
                    auxi['Columns'].append(j)
                    break
        return operation

    def reformat_bcexchange(self,operation, parents):
        tables = []
        columns = []
        for parent in parents:
            tables += parent.auxi["Table"]
            columns += parent.auxi["Columns"]
        auxi = operation.auxi
        auxi["Table"] = tables
        auxi['Columns'] = []
        for i in auxi["Input"]:
            for j in columns:
                if i in j:
                    auxi['Columns'].append(j)
                    break
        return operation


    def reformat(self,operation, structure):
        parent_indices = [i + 1 for i, x in enumerate(structure[operation.idx - 1]) if x == 1]
        parent_operations = [self.operation_dict[i] for i in parent_indices]

        if(operation.operator) == "Scan csv":
            self.reformat_scancsv(operation)
        elif(operation.operator) == "Scan parquet":
            self.reformat_scanparquet(operation)
        elif(operation.operator) == "Filter":
            self.reformat_filter(operation, parent_operations)
        elif(operation.operator) == "Project":
            self.reformat_project(operation, parent_operations)
        elif(operation.operator) == "Exchange":
            self.reformat_exchange(operation, parent_operations)
        elif(operation.operator) == "ReusedExchange":
            self.reformat_reusedexchange(operation, parent_operations)
        elif(operation.operator) == "HashAggregate":
            self.reformat_hashagg(operation, parent_operations)
        elif(operation.operator) == "SortAggregate":
            self.reformat_sortagg(operation, parent_operations)
        elif(operation.operator) == "ObjectHashAggregate":
            self.reformat_ohagg(operation, parent_operations)
        elif(operation.operator) == "Aggregate":
            self.reformat_agg(operation, parent_operations)
        elif(operation.operator) == "Sort":
            self.reformat_sort(operation, parent_operations)
        elif(operation.operator) == "SortMergeJoin":
            self.reformat_smjoin(operation, parent_operations)
        elif(operation.operator) == "BroadcastExchange":
            self.reformat_bcexchange(operation, parent_operations)
        elif(operation.operator) == "BroadcastHashJoin":
            self.reformat_bchjoin(operation, parent_operations)
        elif(operation.operator) == "ShuffledHashJoin":
            self.reformat_schjoin(operation, parent_operations)
        elif(operation.operator) == "LogicalRelation":
            self.reformat_logicalrelation(operation, parent_operations)
        elif(operation.operator) == "Join":
            self.reformat_join(operation, parent_operations)
        elif(operation.operator) == "CartesianProduct":
            self.reformat_cartesianproduct(operation, parent_operations)       
        else:
            print("Unseen operation: {}".format(operation.operator))
            print(operation)

    def encode_operation(self, operation, operation_onehot, table_onehot, column_onehot, normalizes_column, normalizes_table):

        auxi = operation.auxi
        if (auxi.get('Table')):
            table_v = np.concatenate((table_onehot[auxi["Table"][0]], normalizes_table[auxi["Table"][0]]))
        else:
            table_v = np.zeros(len(table_onehot)+len(list(normalizes_table.values())[0]))

        normalizes_column_size = len(list(normalizes_column.values())[0])
        try:
            column_v = [np.concatenate((column_onehot[i], normalizes_column.get(i, np.zeros(normalizes_column_size)))) for i in auxi["Columns"]]
            column_v = np.concatenate(column_v, axis=0)
            operator_v = operation_onehot[operation.operator]
        except:
            column_v = np.zeros(list(column_onehot.values())[0].size)
            operator_v = np.zeros(list(operation_onehot.values())[0].size)
        if(auxi.get("Condition")):
            
            # for sif
            condition = auxi["Condition"]
            condition_v = self.usif_model.infer([(condition, 0)])

        else:
            condition_v = np.zeros(self.condition_dim)
        return operator_v, table_v, column_v, condition_v

    def parse_plans(self, plans):
        # step 1: extract structure and unprocessed details
        plans = [p.split("\n") for p in plans]
        structures = []
        details = []
        skeleton_lens = []
        for plan in plans:
            skeleton, detail = self.split_plan(plan)
            _, nodes = self.parse_skeleton(skeleton)
            skeleton_lens.append(len(skeleton))
            structures.append(self.gen_struct_matrix(nodes))
            details.append(self.parse_detail(detail))

        # step2: parse detail
        for i, o in enumerate(details):
            self.operation_dict = {}
            for d in o:
                self.operation_dict[d.idx] = d
            for operation in o:
                self.reformat(operation, structures[i], self.operation_dict)

        encoded = []
        for query, structure in zip(details, structures):
            q = []
            for i, operation in enumerate(query):
                operator_v, table_v, column_v, condition_v = self.encode_operation(operation, self.operation_onehot, self.table_onehot, self.column_onehot, self.normalizes_column, self.normalizes_table)
                connectivity = structure[i]
                connectivity = np.pad(connectivity, (0, self.max_seq_len - len(connectivity)), "constant", constant_values = (0))
                vs = {"operator": operator_v, "column": column_v, "condition": condition_v, "table": table_v, "structure": connectivity}
                q.append(vs)
            encoded.append(q)

        # step3: 0 padding for operator
        vs = []
        for i in encoded:
            v = []
            for j in i:
                j["column"] = np.pad(j["column"], (0, self.max_column_len - len(j["column"])), "constant", constant_values = (0))
                v.append(np.concatenate((j["operator"], j["table"], j["column"], j["condition"], j["structure"]), axis=None))
            v = np.vstack(v)
            v = torch.tensor(v, dtype=torch.float32)
            vs.append(v)
        
        return vs 

    def parse_plan(self, plan): ## for multi-thread computation
            # step 1: extract structure and unprocessed details
            plan = plan.split("\n") 
            skeleton, detail = self.split_plan(plan)
            _, nodes = self.parse_skeleton(skeleton)
            structure = self.gen_struct_matrix(nodes)
            detail = self.parse_detail(detail)

            # step2: parse detail
            self.operation_dict = {}
            for d in detail:
                self.operation_dict[d.idx] = d
            for operation in detail:
                self.reformat(operation, structure)
            v = []
            for i, operation in enumerate(detail):
                operator_v, table_v, column_v, condition_v = self.encode_operation(operation, self.operation_onehot, self.table_onehot, self.column_onehot, self.normalizes_column, self.normalizes_table)
                connectivity = structure[i]
                connectivity = np.pad(connectivity, (0, self.max_seq_len - len(connectivity)), "constant", constant_values = (0))
                v.append(np.concatenate((operator_v, table_v,  np.pad(column_v, (0, self.max_column_len - len(column_v))),\
                condition_v, connectivity), axis=None))
            v = np.vstack(v)
            v = torch.tensor(v, dtype=torch.float32)
            
            return v[:-1] # to remove the OverwriteByExpression operation

    def pad(self, plan):
        l = plan.shape[0]
        padded = np.pad(plan, ((0, self.max_seq_len-l),(0,0)), "constant", constant_values = (0))
        return torch.tensor(padded), torch.tensor(l/ self.max_seq_len), torch.tensor(self.normalized_resources)

    def parse_plan_padding(self, plan):
        return self.pad(self.parse_plan(plan))