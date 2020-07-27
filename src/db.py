from db_api import *
import json
import os
import datetime


class DBTable(DBTable):

    def __init__(self, name: str, fields: List[DBField], key_field_name: str):
        super().__init__(name, fields, key_field_name)
        self.data_path = f"./{DB_ROOT}/{name}.json"
        my_path = Path(self.data_path)
        if not my_path.is_file():
            dic_ = {}
            for field in fields:
                dic_[field.name] = str(field.type).split(" ")[1][:-2][1:]
            meta_data = {"meta_data": {"name": name, "fields": dic_, "key": key_field_name}}
            with open(self.data_path, "w") as table_file:
                json.dump(meta_data, table_file,default=str)

    def count(self) -> int:
        table_file = open(self.data_path)
        data_table = json.load(table_file)
        return len(data_table.keys())-1

    def insert_record(self, values: Dict[str, Any]) -> None:
        if self.key_field_name not in values.keys():
            raise ValueError
        else:
            table_file = open(self.data_path)
            data_table = json.load(table_file)
            if str(values[self.key_field_name]) in data_table.keys():
                raise ValueError
            with open(self.data_path, "w") as table_file:
                key = values[self.key_field_name]
                values.pop(self.key_field_name)
                data_table[key] = values
                json.dump(data_table, table_file, default=str)

    def delete_record(self, key: Any) -> None:
        print(key)
        table_file = open(self.data_path)
        data_table = json.load(table_file)
        # if key in data_table.keys():
        del data_table[str(key)]
        with open(self.data_path, "w") as table_file:
            json.dump(data_table, table_file,default=str)

    def get_record(self, key: Any) -> Dict[str, Any]:
        table_file = open(self.data_path)
        data_table = json.load(table_file)
        if str(key) not in data_table.keys():
            raise IndexError
        else:
            return data_table[str(key)]

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        table_file = open(self.data_path)
        data_table = json.load(table_file)
        if str(key) not in data_table.keys():
            raise IndexError
        else:
            with open(self.data_path, "w") as table_file:
                data_table[str(key)] =values
                json.dump(data_table, table_file,default=str)


class DataBase(DataBase):
    def __init__(self):
        self.database_path = f"./{DB_ROOT}/my_database.json"

    def create_table(self, table_name: str, fields: List[DBField], key_field_name: str) -> DBTable:
        fields_names = [field.name for field in fields]
        if key_field_name not in fields_names:
            raise ValueError
        new_table = DBTable(table_name, fields, key_field_name)
        check_file = Path(self.database_path)
        if not check_file.is_file():
            with open(self.database_path, "w") as database_file:
                json.dump({new_table.name: new_table.data_path}, database_file,default=str)
        else:
            database_file = open(self.database_path)
            database_data = json.load(database_file)
            database_data[new_table.name] = new_table.data_path
            with open(self.database_path, "w") as database_file:
                json.dump(database_data, database_file,default=str)
        return new_table

    def num_tables(self) -> int:
        check_file = Path(self.database_path)
        if check_file.is_file():
            table_file = open(self.database_path)
            database_data = json.load(table_file)
            return len(database_data.keys())
        return 0

    def create_object(self, file_name) -> DBTable:
        list_fields = []
        file_data = open(file_name)
        data = json.load(file_data)
        for field in list(data["meta_data"]["fields"].keys()):
            list_fields.append(DBField(field, eval(data["meta_data"]["fields"][field])))
        return DBTable(data["meta_data"]["name"], list_fields, data["meta_data"]["key"])

    def get_table(self, table_name: str) -> DBTable:
        table_file = open(self.database_path)
        database_data = json.load(table_file)
        if table_name not in database_data.keys():
            raise IndexError
        else:
            return self.create_object(database_data[table_name])

    def delete_table(self, table_name: str) -> None:
        table_file = open(self.database_path)
        database_data = json.load(table_file)
        database_data.pop(table_name)
        with open(self.database_path, "w") as database_file:
            json.dump(database_data, database_file,default=str)

    def get_tables_names(self) -> List[Any]:
        table_file = open(self.database_path)
        database_data = json.load(table_file)
        return list(database_data.keys())
