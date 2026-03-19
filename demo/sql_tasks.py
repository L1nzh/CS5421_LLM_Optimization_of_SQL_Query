SQL_TASKS = [
    {
        "name": "task_1_filter_pushdown",
        "prompt": """You are an expert SQL query optimizer.

Optimize the following SQL query without changing its semantics.
Target dialect: PostgreSQL.
Output requirements: return only the optimized SQL, no explanation.

SQL:
SELECT
  *
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE c.country = 'US'
  AND o.order_date >= DATE '2023-01-01';
""",
    },
    {
        "name": "task_2_in_to_exists",
        "prompt": """You are an expert SQL query optimizer.

Optimize the following SQL query without changing its semantics.
Target dialect: PostgreSQL.
Output requirements: return only the optimized SQL, no explanation.

SQL:
SELECT
  u.id,
  u.email
FROM users u
WHERE u.id IN (
  SELECT p.user_id
  FROM purchases p
  WHERE p.amount > 100
);
""",
    },
    {
        "name": "task_3_aggregation",
        "prompt": """You are an expert SQL query optimizer.

Optimize the following SQL query without changing its semantics.
Target dialect: PostgreSQL.
Output requirements: return only the optimized SQL, no explanation.

SQL:
SELECT
  d.department_id,
  COUNT(*) AS employee_cnt
FROM employees e
JOIN departments d ON e.department_id = d.department_id
WHERE e.is_active = TRUE
GROUP BY d.department_id
HAVING COUNT(*) >= 10;
""",
    },
]
