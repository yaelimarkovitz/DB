from db_api import *
import json
import os
import datetime
import operator
import hashedindex

ops = {
    '<': operator.lt,
    '<=': operator.le,
    '==': operator.eq,
    '!=': operator.ne,
    '>=': operator.ge,
    '>': operator.gt
}

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
            self.dump_data(meta_data)

    def load_data(self) -> Dict:
        table_file = open(self.data_path)
        return json.load(table_file)

    def dump_data(self, data_to_damp) -> None:
        with open(self.data_path, "w") as table_file:
            json.dump(data_to_damp, table_file, default=str)

    def count(self) -> int:
        data_table = self.load_data()
        return len(data_table.keys()) - 1

    def is_exist(self, key, list_of_keys):
        if key in list_of_keys:
            raise ValueError

    def insert_record(self, values: Dict[str, Any]) -> None:
        if self.key_field_name not in values.keys():
            raise ValueError

        data_table = self.load_data()
        self.is_exist(str(values[self.key_field_name]), data_table.keys())
        key = values[self.key_field_name]
        values.pop(self.key_field_name)
        data_table[key] = values
        self.dump_data(data_table)

    def is_meets_the_criterion(self, record: List, criteria: List[SelectionCriteria]) -> bool:

        for command in criteria:
            operation = ops.get(command.operator)
            if command.field_name == self.key_field_name:
                if not operation(str(record[0]),str(command.value)):
                    return False
            else:
                if not operation(str(record[1][command.field_name]),(command.value)):
                    return False
        return True

    def delete_record(self, key: Any) -> None:
        data_table = self.load_data()
        del data_table[str(key)]
        self.dump_data(data_table)

    def get_record(self, key: Any) -> Dict[str, Any]:
        data_table = self.load_data()
        return data_table[str(key)]

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        data_table = self.load_data()
        records_to_dalete = self.query_table(criteria)
        for record in records_to_dalete:
            del data_table[record["key"]]
        self.dump_data(data_table)

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        data_table = self.load_data()
        if str(key) not in data_table.keys():
            raise KeyError
        else:
            data_table[str(key)] = values
            self.dump_data(data_table)

    # def create_index(self, field_to_index: str) -> None:

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        data_table = self.load_data()
        list_of_values = []
        for record in data_table:
            if record != "meta_data":
                if self.is_meets_the_criterion([record,data_table[record]], criteria):
                    data_table[record]["key"] = record
                    list_of_values.append(data_table[record])
        return list_of_values


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
            self.dump_data({new_table.name: new_table.data_path})
        else:
            database_data = self.load_data()
            database_data[new_table.name] = new_table.data_path
            self.dump_data(database_data)
        return new_table

    def load_data(self) -> Dict:
        database_file = open(self.database_path)
        return json.load(database_file)

    def dump_data(self, data_to_dump: Dict) -> None:
        with open(self.database_path, "w") as database_file:
            json.dump(data_to_dump, database_file, default=str)

    def num_tables(self) -> int:
        check_file = Path(self.database_path)
        if check_file.is_file():
            database_data = self.load_data()
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
        database_data = self.load_data()
        return self.create_object(database_data[table_name])

    def delete_table(self, table_name: str) -> None:

        database_data = self.load_data()
        database_data.pop(table_name)
        self.dump_data(database_data)

    def get_tables_names(self) -> List[Any]:
        database_data = self.load_data()
        return list(database_data.keys())
