import great_expectations as gx

def data_validation(data_source_name,df,expectation_list, layer):
    # creating context
    context = gx.get_context()

    # creating datasource
    try:
        data_source = context.data_sources.get(data_source_name)
    except Exception as e:
        data_source = context.data_sources.add_spark(name=data_source_name)


    # creating data asset
    data_asset_name = f'{data_source_name}_asset'
    try:
        data_asset = data_source.get_asset(data_asset_name)
    except Exception as e:
        data_asset = data_source.add_dataframe_asset(name=data_asset_name)


    # Creating batch definition
    batch_definition_name = f'{data_source_name}_batch_definition'
    try:
        batch_definition = data_asset.get_batch_definition(batch_definition_name)
    except Exception as e:
        batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)


    batch_parameters = {"dataframe": df}


    # Creating expectation suite
    suite_name = 'silver_layer_suite'
    try:
        suite = context.suites.get(suite_name)
    except Exception as e:
        suite = gx.ExpectationSuite(suite_name)
        suite = context.suites.add(suite)

    for expectations in expectation_list:
        suite.add_expectation(expectations)


    # Create and run validation 
    validation_def_name = f"{layer}_layer_validation"

    try:
        validation_definition = context.validation_definitions.get(validation_def_name)

    except Exception as e:
        validation_definition = gx.ValidationDefinition(data=batch_definition, suite=suite, name=validation_def_name)
        validation_definition = context.validation_definitions.add(validation_definition)

    validation_results = validation_definition.run(batch_parameters=batch_parameters)
    return validation_results
