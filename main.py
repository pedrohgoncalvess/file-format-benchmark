from query import main as query_function
from read import main as read_function
from size import main as size_function
from write import main as write_function
import re
import configparser


def read_configs():
    config = configparser.ConfigParser()

    config.read('benchmark.conf')

    validConfigs = {"query": None, "iterations": 1, "operations": None}

    for configAlias in list(validConfigs.keys()):
        try:
            validConfigs.update({configAlias: config.get("Benchmark", configAlias).split("#")[0].strip()})
        except:
            pass

    if validConfigs.get("operations") is not None:
        validOperations = ["size", "query", "read", "write"]
        values = list(set((validConfigs.get("operations").replace(" ", "").split(","))))
        validConfigs.update({"operations": values})
        [validConfigs.update({"operations": None}) if value not in validOperations else None for value in values]

    try:
        validConfigs.update({"iterations": int(validConfigs.get("iterations"))})
    except:
        validConfigs.update({"iterations": 1})

    pattern = re.compile(r'\bselect\b.*\bfrom\b', re.IGNORECASE)
    if validConfigs.get("query") is not None:
        if not pattern.match(validConfigs.get("query")):
            validConfigs.update({"query": None})

    return validConfigs


if __name__ == "__main__":
    configsSet = read_configs()
    configsRelation = {"write": write_function, "read": read_function, "size": size_function, "query": query_function}
    query = configsSet.get("query")
    operationsConfig = configsSet.get("operations")

    if operationsConfig is None:
        operationsToPerform = ["write", "read", "size", "query"]
        if query is None:
            operationsToPerform = operationsToPerform.remove("query")
    else:
        operationsConfig.append("write")
        operationsSet = set(operationsConfig)
        operationsToPerform = []
        for element in operationsSet:
            operationsToPerform.append(element)
            operationsToPerform.sort()
            operationsToPerform.reverse()


    print("Starting benchmark...")
    for iteration in range(configsSet.get("iterations")):
        for opr in operationsToPerform:
            print(f"Starting {opr} operation...")
            if opr == 'query':
                configsRelation.get("query")(query)
            else:
                configsRelation.get(opr)()
