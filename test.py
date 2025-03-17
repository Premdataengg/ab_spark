import re


def convert_abinitio_to_spark_sql(abinitio_expr, input_table="in0", output_table="out"):
    function_mapping = {
        "is_valid": lambda val, type_: "TRY_CAST({} AS {}) IS NOT NULL".format(val, type_.strip('"').upper()),
        "is_null": lambda val: "{} IS NULL".format(val),
        "is_blank": lambda val: "({} IS NULL OR {} = '')".format(val, val),
        "is_defined": lambda val: "{} IS NOT NULL".format(val),
        "length_of": lambda col: "LENGTH({})".format(col),
        "string_filter": lambda col, keep: "REGEXP_REPLACE({}, '[^{}]', '')".format(col, keep),
        "string_filter_out": lambda col, remove: "REGEXP_REPLACE({}, '[{}]', '')".format(col, remove),
        "string_index": lambda col, sub: "INSTR({}, {})".format(col, sub),
        "string_lpad": lambda col, length, ch="'0'": "LPAD({}, {}, {})".format(col, length, ch),
        "string_lrtrim": lambda col: "TRIM({})".format(col),
        "string_ltrim": lambda col: "LTRIM({})".format(col),
        "string_repad": lambda col, length, ch="'0'": "RPAD({}, {}, {})".format(col, length, ch),
        "string_replace": lambda col, old, new: "REPLACE({}, {}, {})".format(col, old, new),
        "string_rindex": lambda col, sub: "LENGTH({}) - INSTR(REVERSE({}), REVERSE({})) + 1".format(col, col, sub),
        "string_substring": lambda col, start, length: "SUBSTRING({}, {}, {})".format(col, start, length),
        "re_get_match": lambda col, regex: "REGEXP_EXTRACT({}, {}, 0)".format(col, regex),
        "re_index": lambda col, regex: "REGEXP_INSTR({}, {})".format(col, regex),
        "re_replace": lambda col, regex, repl: "REGEXP_REPLACE({}, {}, {})".format(col, regex, repl),
        "re_split": lambda col, regex: "SPLIT({}, {})".format(col, regex),
        "string_like": lambda col, pattern: "{} LIKE {}".format(col, pattern),
        "string_join": lambda vec, delim: "ARRAY_JOIN({}, {})".format(vec.replace("vector", "array"), delim),
        "string_prefix": lambda col, prefix: "STARTSWITH({}, {})".format(col, prefix),
        "string_suffix": lambda col, suffix: "ENDSWITH({}, {})".format(col, suffix),
        "string_is_alphabetic": lambda col: "REGEXP_LIKE({}, '^[A-Za-z]+$')".format(col),
        "string_is_numeric": lambda col: "REGEXP_LIKE({}, '^[0-9]+$')".format(col),
        "re_get_range_matches": lambda col, start, end, regex: "SPLIT(SUBSTRING({}, {}, {}), '[^0-9]+')".format(col,
                                                                                                                start,
                                                                                                                int(end) - int(
                                                                                                                    start) + 1),
        "string_concat": lambda *args: "CONCAT({})".format(", ".join(args)),
        "string_upper": lambda col: "UPPER({})".format(col),
        "string_lower": lambda col: "LOWER({})".format(col),
        "today": lambda: "CURRENT_DATE()",
        "now": lambda: "CURRENT_TIMESTAMP()",
        "datetime": lambda fmt, val=None: "DATE_FORMAT({}, 'yyyy-MM-dd HH:mm:ss.SSSSSS')".format(
            val if val else "CURRENT_TIMESTAMP()"),
        "date_to_string": lambda date, fmt: "DATE_FORMAT({}, {})".format(date,
                                                                         fmt.replace("YYYY", "yyyy").replace("HH24",
                                                                                                             "HH").replace(
                                                                             "MI", "mm").replace("SS", "ss")),
        "string_to_date": lambda str_, fmt: "TO_DATE({}, {})".format(str_, fmt.replace("YYYY", "yyyy").replace("HH24",
                                                                                                               "HH").replace(
            "MI", "mm").replace("SS", "ss")),
        "date_add": lambda date, n, unit="day": "DATE_ADD({}, {})".format(date,
                                                                          n) if unit.lower() == "day" else "expr({} + INTERVAL {} {})".format(
            date, n, unit),
        "date_year": lambda date: "YEAR({})".format(date),
        "date_month": lambda date: "MONTH({})".format(date),
        "date_day": lambda date: "DAY({})".format(date),
        "sqrt": lambda val: "SQRT({})".format(val),
        "power": lambda base, exp: "POW({}, {})".format(base, exp),
        "mod": lambda n, d: "MOD({}, {})".format(n, d),
        "round": lambda val, prec="0": "ROUND({}, {})".format(val, prec),
        "reinterpret_as": lambda val, type_: "CAST({} AS {})".format(val, type_.strip('"').upper()),
        "lookup": lambda lookup_file, val: "LEFT JOIN {} ON {} = {}.{}".format(lookup_file.strip('"'), val,
                                                                               lookup_file.strip('"'),
                                                                               val.split('.')[-1]),
        "decimal_strip": lambda col: "CAST(ROUND({}, 2) AS DECIMAL(38,2))".format(col),
        "char_string": lambda code: "CHR({})".format(code),
        "decimal_lpad": lambda num, length, ch="'0'": "LPAD(CAST({} AS STRING), {}, {})".format(num, length, ch),
        "ends_with": lambda col, suffix: "ENDSWITH({}, {})".format(col, suffix),
        "re_match_replace": lambda col, regex, repl: "REGEXP_REPLACE({}, {}, {})".format(col, regex, repl),
        "starts_with": lambda col, prefix: "STARTSWITH({}, {})".format(col, prefix),
        "date_difference_days": lambda date1, date2: "DATEDIFF({}, {})".format(date1, date2),
        "date_add_months": lambda date, months: "ADD_MONTHS({}, {})".format(date, months),
        "datetime_from_unixtime": lambda timestamp: "FROM_UNIXTIME({})".format(timestamp),
        "abs": lambda val: "ABS({})".format(val),
        "ceil": lambda val: "CEIL({})".format(val),
        "floor": lambda val: "FLOOR({})".format(val),
        "to_xml": lambda record: "/* to_xml not natively supported in Spark SQL */ {}".format(record),
        "xml_get_element": lambda xml, elem: "xpath({}, '//{}')".format(xml, elem),
        "to_json": lambda record: "TO_JSON({})".format(record),
        "lookup_count": lambda lookup_file,
                               val: "/* lookup_count requires aggregation */ COUNT(*) FROM {} WHERE {} = {}".format(
            lookup_file.strip('"'), val.split('.')[-1], val),
        "lookup_match": lambda lookup_file, val: "EXISTS(SELECT 1 FROM {} WHERE {} = {})".format(lookup_file.strip('"'),
                                                                                                 val.split('.')[-1],
                                                                                                 val),
        "force_error": lambda msg: "ASSERT(FALSE, {})".format(msg),
        "log_error": lambda msg: "/* log_error not supported in Spark SQL */ {}".format(msg),
    }

    def parse_expression(expr):
        expr = expr.strip()

        if expr.startswith(input_table + '.'):
            return expr.replace(input_table + ".", "")

        func_match = re.match(r'(\w+)\((.*)\)$', expr)
        if func_match:
            func_name, args_str = func_match.groups()

            args = []
            current_arg = ''
            paren_count = 0
            in_quotes = False
            quote_char = None

            for char in args_str:
                if char in ('"', "'"):
                    if in_quotes and char == quote_char:
                        in_quotes = False
                    elif not in_quotes:
                        in_quotes = True
                        quote_char = char
                    current_arg += char
                elif char == ',' and paren_count == 0 and not in_quotes:
                    args.append(current_arg.strip())
                    current_arg = ''
                else:
                    current_arg += char
                    if not in_quotes:
                        if char == '(':
                            paren_count += 1
                        elif char == ')':
                            paren_count -= 1

            if current_arg:
                args.append(current_arg.strip())

            parsed_args = [parse_expression(arg) for arg in args]

            if func_name in ["decimal_lpad"]:
                while len(parsed_args) < 3:
                    parsed_args.append("'0'")

            if func_name in function_mapping:
                return function_mapping[func_name](*parsed_args)
            else:
                return f"/* Unknown function: {func_name} */ {func_name}({', '.join(parsed_args)})"

        return expr

    transformations = [t.strip() for t in abinitio_expr.split(';') if t.strip()]

    spark_expressions = []
    for transform in transformations:
        match = re.match(r'(\w+\.\w+)\s*::\s*(.+)', transform)
        if not match:
            print(f"Skipping invalid transformation: {transform}")
            continue

        out_field, expression = match.groups()
        out_field_name = out_field.split('.')[-1]
        spark_expr = parse_expression(expression)
        spark_expressions.append(f"{spark_expr} AS {out_field_name}")

    return ', '.join(spark_expressions)


# === Test Case ===
if __name__ == "__main__":
    test_input = """
    out.col_a :: '+' ;
    out.col_b :: decimal_lpad(decimal_strip(in0.col_b), 20)
    """
    print("Input Ab Initio Expression:\n", test_input)
    result = convert_abinitio_to_spark_sql(test_input)
    print("\nGenerated Spark SQL Expression:\n", result)
    print("\nFull Spark SQL Query:\nSELECT {} FROM in0".format(result))
