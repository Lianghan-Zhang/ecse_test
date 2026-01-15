from sqlglot import parse_one, transpile

sql = "SELECT a, b, COUNT(*) FROM t GROUP BY ROLLUP (a, b)"

# 解析后再输出 SQL
print(parse_one(sql).sql())
# 预期输出：SELECT a, b, COUNT(*) FROM t GROUP BY ROLLUP (a, b)

# 或者使用 transpile（可切换方言）
print(transpile(sql)[0])
# 预期输出同上
