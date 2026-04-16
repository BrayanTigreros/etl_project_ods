import great_expectations as gx
import pandas as pd

def input_data_validation(df_incautaciones, df_gbif, inp_validate):

    context = gx.get_context(mode="file", project_root_dir=str(inp_validate))
    data_source = context.data_sources.add_or_update_pandas(name="fauna_etl_raw")

    tables = {
        "incautaciones_raw": df_incautaciones,
        "gbif_raw": df_gbif
    }

    results = {}

    for table_name, df in tables.items():

        asset = data_source.add_dataframe_asset(name=f"{table_name}_asset")
        batch_def = asset.add_batch_definition_whole_dataframe(f"{table_name}_batch")
        batch = batch_def.get_batch(batch_parameters={"dataframe": df})

        suite = gx.ExpectationSuite(name=f"{table_name}_suite")

        # INCAUTACIONES RAW 
        if table_name == "incautaciones_raw":

            # Completeness — columnas críticas no deben ser nulas
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="anio"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="departamento"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="situacion"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="cantidad"))

            # Estructura — deben existir todas las columnas renombradas
            suite.add_expectation(gx.expectations.ExpectTableColumnsToMatchSet(
                column_set=[
                    "anio", "departamento", "municipio", "lugar_decomiso",
                    "situacion", "autoridad_que_incauto", "tipo_especie",
                    "nombre_comun", "nombre_cientifico", "cantidad"
                ],
                exact_match=True
            ))

            # Validity — año viene como float (ej: 2.008), debe estar en rango razonable
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
                column="anio", min_value=2008
            ))

            # Validity — cantidad debe ser positiva
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
                column="cantidad", min_value=1
            ))


        # GBIF RAW 
        elif table_name == "gbif_raw":

            # Estructura — columnas que genera extract_gbif
            suite.add_expectation(gx.expectations.ExpectTableColumnsToMatchSet(
                column_set=[
                    "nombre_cientifico_original",
                    "nombre_cientifico_normalizado",
                    "nombre_cientifico_gbif",
                    "usage_key",
                    "reino", "filo", "clase", "orden", "familia", "genero",
                    "estado_taxonomico",
                    "confianza_match",
                    "categoria_iucn"
                ],
                exact_match=True
            ))

            # Completeness — columna origen siempre debe estar
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
                column="nombre_cientifico_original"
            ))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
                column="nombre_cientifico_normalizado"
            ))

            # Validity — confianza entre 0 y 100
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
                column="confianza_match", min_value=0, max_value=100
            ))

            # Validity — categoría IUCN solo puede ser valores conocidos o nulo
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
                column="categoria_iucn",
                value_set=["EX", "EW", "CR", "EN", "VU", "NT", "LC", "DD", "NE", None]
            ))


        context.suites.add_or_update(suite)
        validation_result = batch.validate(suite)
        results[table_name] = validation_result

        # Score por tabla
        stats = validation_result["statistics"]
        score = stats["success_percent"]
        passed = stats["successful_expectations"]
        total = stats["evaluated_expectations"]
        print(f"\n[{table_name}] DQ Score: {score:.1f}% — Passed: {passed}/{total}")

    # Score global
    print(f"\n{'='*50}")
    total_passed = sum(r["statistics"]["successful_expectations"] for r in results.values())
    total_evaluated = sum(r["statistics"]["evaluated_expectations"] for r in results.values())
    global_score = (total_passed / total_evaluated * 100) if total_evaluated > 0 else 0
    print(f"DQ Score global (input): {global_score:.1f}%")
    print(f"Total passed: {total_passed} / {total_evaluated}")
    print(f"\n{'='*50}")

    context.build_data_docs()

    return results