SELECT COUNT(*) FROM title t,movie_companies mc,movie_info mi,movie_keyword mk WHERE t.id=mc.movie_id AND t.id=mi.movie_id AND t.id=mk.movie_id AND t.kind_id<7 AND mc.company_id<11471 AND mc.company_type_id=1 AND mi.info_type_id<2;