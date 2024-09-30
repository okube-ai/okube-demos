for scope in dbutils.secrets.listScopes():
    for secret in dbutils.secrets.list(scope.name):
        print(f"{scope.name}/{secret.key}")
