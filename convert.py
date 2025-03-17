import re


def convert_abinitio_to_spark_sql(abinitio_expr, input_table="in0", output_table="out"):
    """
    Convert semicolon-separated or comma-separated Ab Initio transformations to comma-separated Spark SQL expressions.

    Args:
        abinitio_expr (str): Ab Initio transformation rules (using :: syntax).
        input_table (str): Input table name (default: 'in0').
        output_table (str): Output table name (default: 'out').

    Returns:
        str: Comma-separated Spark SQL expressions.
    """
    # Full function mapping from Ab Initio to Spark SQL
    function_mapping = {
        "is_valid": lambda val, type_: "TRY_CAST(" + val + " AS " + type_.strip('"').upper() + ") IS NOT NULL",
        "is_null": lambda val: val + " IS NULL",
        "is_blank": lambda val: "(" + val + " IS NULL OR " + val + " = '')",
        "is_defined": lambda val: val + " IS NOT NULL",
        "length_of": lambda col: "LENGTH(" + col + ")",
        "string_filter": lambda col, keep: "REGEXP_REPLACE(" + col + ", '[^" + keep + "]', '')",
        "string_filter_out": lambda col, remove: "REGEXP_REPLACE(" + col + ", '[" + remove + "]', '')",
        "string_index": lambda col, sub: "INSTR(" + col + ", " + sub + ")",
        "string_lpad": lambda col, len, ch: "LPAD(" + col + ", " + len + ", " + ch + ")",
        "string_lrtrim": lambda col: "TRIM(" + col + ")",
        "string_ltrim": lambda col: "LTRIM(" + col + ")",
        "string_repad": lambda col, len, ch: "RPAD(" + col + ", " + len + ", " + ch + ")",
        "string_replace": lambda col, old, new: "REPLACE(" + col + ", " + old + ", " + new + ")",
        "string_rindex": lambda col, sub: "LENGTH(" + col + ") - INSTR(REVERSE(" + col + "), REVERSE(" + sub + ")) + 1",
        "string_substring": lambda col, start, length: "SUBSTRING(" + col + ", " + start + ", " + length + ")",
        "re_get_match": lambda col, regex: "REGEXP_EXTRACT(" + col + ", " + regex + ", 0)",
        "re_index": lambda col, regex: "REGEXP_INSTR(" + col + ", " + regex + ")",
        "re_replace": lambda col, regex, repl: "REGEXP_REPLACE(" + col + ", " + regex + ", " + repl + ")",
        "re_split": lambda col, regex: "SPLIT(" + col + ", " + regex + ")",
        "string_like": lambda col, pattern: col + " LIKE " + pattern,
        "string_join": lambda vec, delim: "ARRAY_JOIN(" + vec.replace("vector", "array") + ", " + delim + ")",
        "string_prefix": lambda col, prefix: "STARTSWITH(" + col + ", " + prefix + ")",
        "string_suffix": lambda col, suffix: "ENDSWITH(" + col + ", " + suffix + ")",
        "string_is_alphabetic": lambda col: "REGEXP_LIKE(" + col + ", '^[A-Za-z]+$')",
        "string_is_numeric": lambda col: "REGEXP_LIKE(" + col + ", '^[0-9]+$')",
        "re_get_range_matches": lambda col, start, end, regex: "SPLIT(SUBSTRING(" + col + ", " + start + ", " + str(
            int(end) - int(start) + 1) + "), '[^0-9]+')",
        "string_concat": lambda *args: "CONCAT(" + ", ".join(args) + ")",
        "string_upper": lambda col: "UPPER(" + col + ")",
        "string_lower": lambda col: "LOWER(" + col + ")",
        "today": lambda: "CURRENT_DATE()",
        "now": lambda: "CURRENT_TIMESTAMP()",
        "datetime": lambda fmt, val=None: "DATE_FORMAT(" + (
            val if val else "CURRENT_TIMESTAMP()") + ", 'yyyy-MM-dd HH:mm:ss.SSSSSS')",
        "date_to_string": lambda date, fmt: "DATE_FORMAT(" + date + ", " + fmt.replace("YYYY", "yyyy").replace("HH24",
                                                                                                               "HH").replace(
            "MI", "mm").replace("SS", "ss") + ")",
        "string_to_date": lambda str_, fmt: "TO_DATE(" + str_ + ", " + fmt.replace("YYYY", "yyyy").replace("HH24",
                                                                                                           "HH").replace(
            "MI", "mm").replace("SS", "ss") + ")",
        "date_add": lambda date, n,
                           unit="day": "DATE_ADD(" + date + ", " + n + ")" if unit.lower() == "day" else "expr(" + date + " + INTERVAL " + n + " " + unit + ")",
        "date_year": lambda date: "YEAR(" + date + ")",
        "date_month": lambda date: "MONTH(" + date + ")",
        "date_day": lambda date: "DAY(" + date + ")",
        "sqrt": lambda val: "SQRT(" + val + ")",
        "power": lambda base, exp: "POW(" + base + ", " + exp + ")",
        "mod": lambda n, d: "MOD(" + n + ", " + d + ")",
        "round": lambda val, prec="0": "ROUND(" + val + ", " + prec + ")",
        "reinterpret_as": lambda val, type_: "CAST(" + val + " AS " + type_.strip('"').upper() + ")",
        "lookup": lambda lookup_file, val: "LEFT JOIN " + lookup_file.strip(
            '"') + " ON " + val + " = " + lookup_file.strip('"') + "." + val.split('.')[-1],
        "decimal_strip": lambda col: "CAST(" + col + " AS DECIMAL(10,2))",
        "char_string": lambda code: "CHR(" + code + ")",
        "decimal_lpad": lambda num, len, ch: "LPAD(CAST(" + num + " AS STRING), " + len + ", " + ch + ")",
        "ends_with": lambda col, suffix: "ENDSWITH(" + col + ", " + suffix + ")",
        "re_match_replace": lambda col, regex, repl: "REGEXP_REPLACE(" + col + ", " + regex + ", " + repl + ")",
        "starts_with": lambda col, prefix: "STARTSWITH(" + col + ", " + prefix + ")",
        "date_difference_days": lambda date1, date2: "DATEDIFF(" + date1 + ", " + date2 + ")",
        "date_add_months": lambda date, months: "ADD_MONTHS(" + date + ", " + months + ")",
        "datetime_from_unixtime": lambda timestamp: "FROM_UNIXTIME(" + timestamp + ")",
        "abs": lambda val: "ABS(" + val + ")",
        "ceil": lambda val: "CEIL(" + val + ")",
        "floor": lambda val: "FLOOR(" + val + ")",
        "to_xml": lambda record: "/* to_xml not natively supported in Spark SQL */ " + record,
        "xml_get_element": lambda xml,
                                  elem: "/* xml_get_element requires UDF or external library */ xpath(" + xml + ", '//" + elem + "')",
        "to_json": lambda record: "TO_JSON(" + record + ")",
        "lookup_count": lambda lookup_file,
                               val: "/* lookup_count requires aggregation */ COUNT(*) FROM " + lookup_file.strip(
            '"') + " WHERE " + val.split('.')[-1] + " = " + val,
        "lookup_match": lambda lookup_file, val: "EXISTS(SELECT 1 FROM " + lookup_file.strip('"') + " WHERE " +
                                                 val.split('.')[-1] + " = " + val + ")",
        "force_error": lambda msg: "ASSERT(FALSE, " + msg + ")",
        "log_error": lambda msg: "/* log_error not supported in Spark SQL */ " + msg
    }

    # Split transformations by comma or semicolon and filter out empty entries
    transformations = [t.strip().strip('"') for t in re.split(r'[;,]', abinitio_expr) if t.strip()]

    spark_expressions = []

    for transform in transformations:
        # Match output field and expression (e.g., "out.col_a :: in0.col_a")
        match = re.match(r'(\w+\.\w+)\s*::\s*(.+)', transform)
        if not match:
            print(f"Skipping invalid transformation: {transform}")
            continue

        out_field, expression = match.groups()
        out_field_name = out_field.split('.')[-1]  # Extract field name after 'out.'

        # Handle function calls (e.g., "(datetime('YYYY-MM-DD'))in0.col_b")
        func_match = re.match(r'\((\w+)\((.*?)\)\)(.+)', expression)
        if func_match:
            func_name, args_str, remaining = func_match.groups()
            args = [arg.strip() for arg in args_str.split(',')]

            if func_name in function_mapping:
                # Apply the mapped function with arguments
                if len(args) == 1 and remaining:
                    # For functions like datetime applied to a column
                    spark_expr = function_mapping[func_name](args[0], remaining.replace(f'{input_table}.', ''))
                else:
                    spark_expr = function_mapping[func_name](*args)
            else:
                spark_expr = expression + " /* Unmapped function: " + func_name + " */"
        else:
            # Handle simple expressions (e.g., in0.col_a)
            spark_expr = expression.replace(f'{input_table}.', '')  # Remove input table prefix

        # Construct Spark SQL expression
        spark_expressions.append(spark_expr + " AS " + out_field_name)

    # Join all expressions with commas
    return ', '.join(spark_expressions)


# Example usage with your input
if __name__ == "__main__":
    # "out.col_a :: in0.col_a";
    # "out.col_b :: (datetime('YYYY-MM-DD'))in0.col_b"
    # Your Ab Initio transformations
    abinitio_input = """
    out.col_a :: '+' ;
    out.col_b :: decimal_lpad(decimal_strip(in.col_b), 20)
    """

    # Convert to Spark SQL
    spark_output = convert_abinitio_to_spark_sql(abinitio_input, input_table="in0", output_table="out")
    print("Generated Spark SQL expressions:")
    print(spark_output)

    # Full Spark SQL query
    full_query = "SELECT " + spark_output + " FROM in0"
    print("\nFull Spark SQL query:")
    print(full_query)