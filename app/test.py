# Test cases for the convert_abinitio_to_spark_sql function
from app.convert import convert_abinitio_to_spark_sql


def run_test_cases():
    test_cases = [
        {
            "description": "Simple literal assignment",
            "input": "out.col_a :: '+';",
            "expected": "'+' AS col_a"
        },
        {
            "description": "Nested functions with decimal_lpad and decimal_strip",
            "input": "out.col_b :: decimal_lpad(decimal_strip(in0.col_b), 20);",
            "expected": "LPAD(CAST(CAST(ROUND(col_b, 2) AS DECIMAL(38,2)) AS STRING), 20, '0') AS col_b"
        },
        {
            "description": "Function with default argument",
            "input": "out.col_c :: string_lpad(in0.col_c, 10);",
            "expected": "LPAD(col_c, 10, '0') AS col_c"
        },
        {
            "description": "Function with all arguments provided",
            "input": "out.col_d :: string_lpad(in0.col_d, 10, '*');",
            "expected": "LPAD(col_d, 10, '*') AS col_d"
        },
        {
            "description": "Unknown function handling",
            "input": "out.col_e :: unknown_func(in0.col_e);",
            "expected": "/* Unknown function: unknown_func */ unknown_func(col_e) AS col_e"
        },
        {
            "description": "Multiple transformations",
            "input": "out.col_f :: string_upper(in0.col_f); out.col_g :: string_lower(in0.col_g);",
            "expected": "UPPER(col_f) AS col_f, LOWER(col_g) AS col_g"
        },
        {
            "description": "Function with nested arguments",
            "input": "out.col_h :: string_replace(string_upper(in0.col_h), 'A', 'B');",
            "expected": "REPLACE(UPPER(col_h), 'A', 'B') AS col_h"
        },
        {
            "description": "Function with numeric literal argument",
            "input": "out.col_i :: power(in0.col_i, 2);",
            "expected": "POW(col_i, 2) AS col_i"
        },
        {
            "description": "Function with string literal argument",
            "input": "out.col_j :: string_concat(in0.col_j, 'suffix');",
            "expected": "CONCAT(col_j, 'suffix') AS col_j"
        },
        {
            "description": "Function with multiple arguments",
            "input": "out.col_k :: string_substring(in0.col_k, 1, 3);",
            "expected": "SUBSTRING(col_k, 1, 3) AS col_k"
        }
    ]

    for case in test_cases:
        result = convert_abinitio_to_spark_sql(case["input"])
        assert result == case["expected"], f"Test failed for: {case['description']}\nExpected: {case['expected']}\nGot: {result}"
        print(f"Test passed for: {case['description']}")

if __name__ == "__main__":
    run_test_cases()
