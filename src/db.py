from db_api import *
import json


class DBTable(DBTable):

    def __init__(self, name: str, fields: List[DBField], key_field_name: str):
        super(self).__init__(name, fields, key_field_name)
        self.data_path = f"/{name}.json"

    def count(self) -> int:
        table_file = open(self.data_path)
        data_table = json.load(table_file)
        return len(data_table[0])

    def insert_record(self, values: Dict[str, Any]) -> None:

        if self.key_field_name not in values.keys():
            raise IndexError  # TODO check if to raise error or not

        else:
            table_file = open(self.data_path)
            data_table = json.load(table_file)
            with open(self.data_path, "w") as table_file:
                data_table[0][values[self.key_field_name]] = values.pop(self.key_field_name)
                json.dump(data_table,table_file)

    def delete_record(self, key: Any) -> None:
        table_file = open(self.data_path)
        data_table = json.load(table_file)
        with open(self.data_path, "w") as table_file:
            data_table[0].pop(key)
            json.dump(data_table, table_file)

    def get_record(self, key: Any) -> Dict[str, Any]:
        table_file = open(self.data_path)
        data_table = json.load(table_file)
        if key not in data_table[0]:
            raise IndexError
        else:
            return data_table[0][key]

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        table_file = open(self.data_path)
        data_table = json.load(table_file)
        if key not in data_table[0]:
            raise IndexError
        else:
            with open(self.data_path, "w") as table_file:
                data_table[0][key] = values.pop(key)
                json.dump(data_table, table_file)


class DataBase(DataBase):
    def __init__(self):
        self.database_path = "./"


