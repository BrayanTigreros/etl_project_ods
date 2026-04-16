import great_expectations as gx
import pandas as pd

def output_data_validation(transformed_data, out_validate):

    context = gx.get_context(mode="file", project_root_dir=str(out_validate))

    data_source = context.data_sources.add_or_update_pandas(name="incautaciones_check")

    suites = {}
    results = {}

    tables = {
        "dim_tiempo": transformed_data["dim_tiempo"],
        "dim_ubicacion": transformed_data["dim_ubicacion"],
        "dim_especie": transformed_data["dim_especie"],
        "dim_autoridad": transformed_data["dim_autoridad"],
        "fact_incautaciones": transformed_data["fact_incautaciones"],
    }

    for table_name, df in tables.items():

        asset = data_source.add_dataframe_asset(name=f"{table_name}_asset")
        batch_def = asset.add_batch_definition_whole_dataframe(f"{table_name}_batch")
        batch = batch_def.get_batch(batch_parameters={"dataframe": df})

        suite = gx.ExpectationSuite(name=f"{table_name}_suite")

        # DIM_TIEMPO 
        if table_name == "dim_tiempo":

            # Completeness
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="tiempo_key"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="anio"))

            # Uniqueness
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="tiempo_key"))

            # Validity — años razonables para el dataset
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
                column="anio", min_value=2008
            ))


        # DIM_UBICACION 
        elif table_name == "dim_ubicacion":

            # Completeness — nulos fueron reemplazados por "DESCONOCIDO"
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="ubicacion_key"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="departamento"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="municipio"))

            # Uniqueness
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="ubicacion_key"))


        # DIM_ESPECIE 
        elif table_name == "dim_especie":

            # Completeness — nulos reemplazados por "DESCONOCIDO"
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="especie_key"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="tipo_especie"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="nombre_comun"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="nombre_cientifico"))

            # Uniqueness
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="especie_key"))

            # Validity 
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="product",
            value_set=["AVES", "FAUNA ACUATICA", "MAMIFEROS", "REPTILES", "ANFIBIOS", 
                       "ARACNIDOS", "ESPECIMENES", "CRUSTACEOS", "CRUSTACEOS", "MOLUSCOS", "DESCONOCIDO", "PRODUCTOS", "ARTROPODOS"]
            ))

            
            # La dimensión fue enriquecida con GBIF, debe tener más columnas
            suite.add_expectation(gx.expectations.ExpectTableColumnsToMatchSet(
                column_set=["especie_key", "tipo_especie", "nombre_comun", "nombre_cientifico"],
                exact_match=False  # GBIF puede agregar columnas adicionales
            ))

        # DIM_AUTORIDAD 
        elif table_name == "dim_autoridad":

            # Completeness 
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="autoridad_key"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="autoridad_que_incauto"))

            # Uniqueness
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="autoridad_key"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="autoridad_que_incauto"))


        # FACT_INCAUTACIONES 
        elif table_name == "fact_incautaciones":

            # Completeness — todas las FK deben estar presentes (merge left, no deben quedar nulos)
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="tiempo_key"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="ubicacion_key"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="especie_key"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="autoridad_key"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="situacion"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="cantidad"))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))

            # Validity — cantidad debe ser positiva
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
                column="cantidad", min_value=1
            ))

            # Uniquess id unico
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="id"))

            # Validity — FK deben ser enteros positivos (referencial)
            for fk in ["tiempo_key", "ubicacion_key", "especie_key", "autoridad_key"]:
                suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
                    column=fk, min_value=1
                ))


            # Integridad referencial — FK deben existir en sus dimensiones
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
                column="tiempo_key",
                value_set=transformed_data["dim_tiempo"]["tiempo_key"].tolist()
            ))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
                column="ubicacion_key",
                value_set=transformed_data["dim_ubicacion"]["ubicacion_key"].tolist()
            ))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
                column="especie_key",
                value_set=transformed_data["dim_especie"]["especie_key"].tolist()
            ))
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
                column="autoridad_key",
                value_set=transformed_data["dim_autoridad"]["autoridad_key"].tolist()
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
    print(f"DQ Score global (output): {global_score:.1f}%")
    print(f"Total passed: {total_passed} / {total_evaluated}")
    print(f"\n{'='*50}")

    context.build_data_docs()

    return results