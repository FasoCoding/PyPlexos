
class XMLFileError(Exception):
    def __init__(self) -> None:
        super().__init__("Error XML File: No model file on zip solution.")

class BinFileError(Exception):
    def __init__(self) -> None:
        super().__init__("Error BIN FIle: No BIN files on zip solution.")

class ZipFileError(Exception):
    def __init__(self) -> None:
        super().__init__("Error Zip FIle: The specified ZIP file path does not exist.")

class TableNotFoundError(UnboundLocalError, BaseException):
    def __init__(self, table_name: str) -> None:
        super().__init__(f"Error Table: The table {table_name} is not part of the schema.")