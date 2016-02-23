SELECT a.account_id FROM bratchev.allocations_5715 a left outer join bratchev.darwin_users_5715 b on a.account_id = b.account_id where b.account_id is null
