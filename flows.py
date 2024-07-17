# o Flow propriamente dito.

from prefect import Flow, Parameter

from tasks import (
    connect_to_db,
    execute_query,
    download_data,
    parse_data,
    save_report,
)

with Flow("Users report") as flow:

    # Parâmetros
    n_users = Parameter("n_users", default=10)

    # Tasks
    data = download_data(n_users)
    dataframe = parse_data(data)
    save_report(dataframe)

with Flow("PostgreSQL Connection Flow") as flow:
    conn = connect_to_db()
    results = execute_query(conn)
