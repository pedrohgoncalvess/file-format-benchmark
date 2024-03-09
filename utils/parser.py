def formats_parser(configs:dict[str:any]) -> dict[str:any]:
    validConfigs = {}
    defaultFormats = ['avro','csv','json','orc','parquet']
    if configs.get("formats") is None:
        return {"formats":defaultFormats}
    values = list(set((configs.get("formats").replace(" ", "").split(","))))
    validConfigs.update({"formats":values})
    [validConfigs.update({"formats": defaultFormats}) if value not in defaultFormats else None for value in values]
    return validConfigs
    
def operations_parser(configs:dict[str:any]) -> dict[str:any]:
    validConfigs = {}
    if configs.get("operations") is not None:
        defaultOperations = ["size", "query", "read", "write", "all"]
        values = list(set((configs.get("operations").replace(" ", "").split(","))))
        validConfigs.update({"operations": values})
        [validConfigs.update({"operations": defaultOperations}) if value not in defaultOperations else None for value in values]
    return validConfigs

def iterations_parser(configs:dict[str:any]) -> dict[str:any]:
    try:
        return {"iterations": int(configs.get("iterations"))}
    except:
        return {"iterations": 1}
    
    
def query_parser(configs:dict[str:any]) -> dict[str:any]:
    import re
    pattern = re.compile(r'\bselect\b.*\bfrom\b', re.IGNORECASE)
    if configs.get("query") is not None:
        if not pattern.match(configs.get("query")):
            return {"query": None}
        return {"query":configs.get("query")}
    
    

def read_configs() -> dict[str:any]:
    import configparser
    
    config = configparser.ConfigParser()

    config.read('benchmark.conf')

    validConfigs = {"query": None, "iterations": 1, "operations": None, "formats":None}

    for configAlias in list(validConfigs.keys()):
        try:
            validConfigs.update({configAlias: config.get("Benchmark", configAlias).split("#")[0].strip()})
        except:
            pass

    parsers = [formats_parser, operations_parser, iterations_parser, query_parser]
    for parser in parsers:
        validConfigs.update(parser(validConfigs))

    return validConfigs