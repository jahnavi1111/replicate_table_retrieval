import logging
import duckdb
import os
import json
from utils.response import Response, ResponseStatus
#from sentence_transformers import SentenceTransformer

logger = logging.getLogger("main")

out_path = os.path.join(os.getcwd(), "pneuma-out")
os.makedirs(out_path, exist_ok=True)

out_path = os.path.abspath(out_path)
db_path = os.path.join(out_path, "storage.db")
index_path = os.path.join(out_path, "indexes")


# READ TABLE DATA

def read_table_folder(
        folder_path: str, creator: str, accept_duplicates: bool = False
    ) -> Response:
        """
        Reads a folder and registers all of its tables to the database.

        ## Args
        - **folder_path** (`str`): The path to a folder containing tables.
        - **creator** (`str`): The creator of the file.
        - **accept_duplicates** (`bool`): Option to allow duplicate tables or not.

        ## Returns
        - `Response`: A `Response` object of the process.
        """
        logger.info(f"Reading folder {folder_path}")
        paths = [os.path.join(folder_path, f) for f in os.listdir(folder_path)]
        data = []
        for path in paths:
            logger.info(f"=> Processing {path}")

            # If the path is a folder, recursively read the folder.
            if os.path.isdir(path):
                response = read_table_folder(path, creator, accept_duplicates)
                logger.info(response.message)
                data.extend(response.data["tables"])
                continue

            response = read_table_file(path, creator, accept_duplicates)
            logger.info(
                f"==> Processing table {path} {response.status.value}: {response.message}"
            )
            data.append(response.data)

        file_count = len(data)
        return Response(
            status=ResponseStatus.SUCCESS,
            message=f"{file_count} files in folder {folder_path} has been processed.",
            data={"file_count": file_count, "tables": data},
        )


def read_table_file(
        path: str,
        creator: str,
        accept_duplicates: bool = False,
    ) -> Response:
        """
        Reads a table file (CSV or Parquet), registers it in the database, and
        updates if an existing table has the same ID.

        ## Args
        - **path** (`str`): The path to a specific table file (`CSV` or `parquet`).
        - **creator** (`str`): The creator of the file.
        - **accept_duplicates** (`bool`): Option to allow duplicate tables or not.

        ## Returns
        - `Response`: A `Response` object of the process.
        """
        try:
            with duckdb.connect(db_path) as connection:
                # Index -1 to get the file extension, then slice [1:] to remove the dot.
                file_type = os.path.splitext(path)[-1][1:]

                if file_type not in ["csv", "parquet"]:
                    return Response(
                        status=ResponseStatus.ERROR,
                        message="Invalid file type. Please use 'csv' or 'parquet'.",
                    )

                # If the path contains single quotes, we need to escape them to avoid
                # breaking the SQL query.
                path = path.replace("'", "''")
                name = path.split("/")[-1][:-4]
                table = connection.sql(
                    f"""SELECT *
                        FROM read_csv(
                            '{path}',
                            auto_detect=True,
                            header=True,
                            ignore_errors=True
                        )"""
                )
                table_hash = connection.sql(
                    f"""SELECT md5(string_agg(tbl::text, ''))
                    FROM read_csv(
                        '{path}',
                        auto_detect=True,
                        header=True,
                        ignore_errors=True
                    ) AS tbl"""
                ).fetchone()[0]

                # For ease of keeping track of IDs, we replace backslashes (Windows) with
                # forward slashes (everything else) to make the path (therefore, ID) consistent.
                path = path.replace("\\", "/")

                # We want to avoid double quotes on table names, so we change them to single quotes.
                path = path.replace('"', "''")

                if not accept_duplicates:
                    # Check if table with the same hash already exist
                    table_exist = connection.sql(
                        f"SELECT id FROM table_status WHERE hash = '{table_hash}'"
                    ).fetchone()
                    if table_exist:
                        return Response(
                            status=ResponseStatus.ERROR,
                            message=f"This table already exists in the database with id {table_exist}.",
                        )

                # Check if a table with the same ID already exists
                existing_entry = connection.sql(
                    f"SELECT table_name FROM table_status WHERE id = '{path}'"
                ).fetchone()

                if existing_entry:
                    old_table_name = existing_entry[0]
                    connection.sql(f"DROP TABLE IF EXISTS \"{old_table_name}\"")
                    connection.sql(
                        f"DELETE FROM table_status WHERE id = '{path}'"
                    )

                # The double quote is necessary to consider the path, which may contain
                # full stop that may mess with schema as a single string. Having single quote
                # inside breaks the query, so having the double quote INSIDE the single quote
                # is the only way to make it work.

                # Because we are using double quotes for the table creation, we need to unescape
                # the single quote so the table name fits the ID that is stored in table_status.
                create_path = path.replace("''", "'")
                table.create(f'"{create_path}"')

                connection.sql(
                    f"""INSERT INTO table_status (id, table_name, status, creator, hash)
                    VALUES ('{path}', '{name}', '{TableStatus.REGISTERED}', '{creator}', '{table_hash}')"""
                )
                
                return Response(
                    status=ResponseStatus.SUCCESS,
                    message=f"Table with ID: {path} has been added to the database.",
                    data={"table_id": path, "table_name": name},
                )
        except Exception as e:
            return Response(
                status=ResponseStatus.ERROR,
                message=f"Error connecting to database: {e}",
            )
def add_tables(
        path: str,
        creator: str,
        source: str = "file",
        s3_region: str = None,
        s3_access_key: str = None,
        s3_secret_access_key: str = None,
        accept_duplicates: bool = False,
    ) -> str:
        """
        Adds tables into the database.

        ## Args
        - **path** (`str`): The path to a specific table file/folder (`CSV` or
        `parquet`).
        - **creator** (`str`): The creator of the file.
        - **source** (`str`): The dataset source (either `file` or `s3`).
        - **s3_region** (`int`): Amazon S3 region.
        - **s3_access_key** (`int`): Amazon S3 access key.
        - **s3_secret_access_key** (`int`): Amazon S3 secret access key.
        - **accept_duplicates** (`bool`): Option to accept duplicate tables or not.

        ## Returns
        - `str`: A JSON string representing the result of the process (`Response`).
        """

        if os.path.isfile(path):
            return read_table_file(path, creator, accept_duplicates).to_json()
        if os.path.isdir(path):
            return read_table_folder(path, creator, accept_duplicates).to_json()

        return Response(
            status=ResponseStatus.ERROR,
            message=f"Invalid path: {path}",
        ).to_json()      


data_path = "data_src/sample_data/csv"
response = add_tables(path=data_path, creator="demo_user")
print(response)