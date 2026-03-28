python -m cli.optimization_pipeline_cli \
--dsn "postgresql://postgres:abcd1234@localhost:5432/as2" \
--raw-query "
SELECT e.empid, e.lname, p.salary
FROM employee e
JOIN payroll p ON p.empid = e.empid
WHERE e.empid = '00000'
" \
--model gpt-4o-mini \
--prompt-strategy P1_ENGINE \
--reasoning-mode DIRECT \
--candidate-count 3 \
--comparison-strategy hash \
--stream-batch-size 1000