from operations.query import main as query_function
from operations.read import main as read_function
from operations.size import main as size_function
from operations.write import main as write_function
from operations.all import main as all_function
from utils.parser import read_configs


if __name__ == "__main__":
    configsSet = read_configs()
    configsRelation = {"write": write_function, "read": read_function, "size": size_function, "query": query_function, "all": all_function}
    query = configsSet.get("query")
    operationsConfig = configsSet.get("operations")

    if operationsConfig is None:
        operationsToPerform = ["write", "read", "size", "query", "all"]
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
            if opr == 'query' or opr=='all':
                configsRelation.get(opr)(query)
            else:
                configsRelation.get(opr)()
