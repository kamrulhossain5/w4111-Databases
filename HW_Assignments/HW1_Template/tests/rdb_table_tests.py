import src.RDBDataTable as RDBDataTable
import json

def init_data_table_test(table_name=None, connect_info=None, key_columns=None):
    #load
    table_name = "Batting"
    key_columns = ["playerID","yearID","stint","teamID"]
    rdb_data_table = RDBDataTable.RDBDataTable(table_name=table_name, key_columns=key_columns)
    print("Loaded table = \n", rdb_data_table)
    return rdb_data_table

def find_by_primary_key_test(data_table, key_fields=None, field_list=None):
    print("\n\n")
    print("******************** " + "find_by_primary_key_test" + " ********************")

    key_fields = ["beckero01","1996","1","COL"]
    field_list = ["playerID", "teamID", "CS","SB"]

    print("Key =", key_fields)
    print("Fields= ", field_list)
    res = data_table.find_by_primary_key(key_fields=key_fields, field_list=field_list)
    print("Result = \n", res)
    print("******************** " + "end find_by_primary_key_test" + " ********************")

def find_by_template_test(data_table, template=None, field_list=None):
    print("\n\n")
    print("******************** " + "find_by_template_test" + " ********************")

    template = {'yearID': '1902', 'stint':'1', 'teamID':'CHN'}
    field_list = ["playerID", "yearID", "stint", "teamID"]
    print("Template =", json.dumps(template, indent=2))
    print("Field list = ", field_list)
    res = data_table.find_by_template(template)
    print("Result = \n", json.dumps(res, indent=2))
    print("******************** " + "end find_by_template_test" + " ********************")

def delete_by_key_test(data_table, key_fields=None):
    print("\n\n")
    print("******************** " + "delete_by_key_test" + " ********************")

    insertedRecord = {"playerID":"Hope", "yearID":"1997", "stint":"1", "teamID":"Columbia",
                 "lgID":"NN", "G":"9", "AB":"8", "R":"7","H":"6", "2B":"5", "3B":"4",
                 "HR":"3", "RBI":"2", "SB":"1","CS":"0", "BB":"0", "SO":"0", "IBB":"0",
                 "HBP":"0", "SH":"0", "SF":"0", "GIDP":"0"}
    data_table.insert(insertedRecord)

    key_fields = ["Hope", "1997", "1", "Columbia"]
    r1 = data_table.find_by_primary_key(key_fields=key_fields)
    print("Details for datatable before deletion = \n", r1)

    print("\nDeleting ['Hope', '1997', '1', 'Columbia']:")
    affected_num = data_table.delete_by_key(key_fields=key_fields)
    print("Delete returned = \n", affected_num)

    r2 = data_table.find_by_primary_key(key_fields=key_fields)
    print("Details for datatable after delete = \n", r2)
    print("******************** " + "end delete_by_key_test" + " ********************")

def delete_by_template_test(data_table, template=None):
    print("\n\n")
    print("******************** " + "delete_by_template_test" + " ********************")

    insertedRecord = {"playerID":"Hope2", "yearID":"1997", "stint":"1", "teamID":"Columbia",
                 "lgID":"NN", "G":"9", "AB":"8", "R":"7","H":"6", "2B":"5", "3B":"4",
                 "HR":"3", "RBI":"2", "SB":"1","CS":"0", "BB":"0", "SO":"0", "IBB":"0",
                 "HBP":"0", "SH":"0", "SF":"0", "GIDP":"0"}
    data_table.insert(insertedRecord)

    template = {"teamID":"Columbia"}
    r1 = data_table.find_by_template(template=template)
    print("Details for teamID : 'Columbia' = \n", json.dumps(r1, indent=2))

    affected_num = data_table.delete_by_template(template=template)
    print("Delete returned = \n", affected_num)

    r2 = data_table.find_by_template(template=template)
    print("Details for teamID 'Columbia' after delete = \n", json.dumps(r2, indent=2))

    print("******************** " + "end delete_by_template_test" + " ********************")

def update_by_key_test(data_table, key_fields=None, new_values=None):
    print("\n\n")
    print("******************** " + "update_by_key_test" + " ********************")

    key_fields = ["kammwi01", "1923"]
    r1 = data_table.find_by_primary_key(key_fields=key_fields)
    print("Details for ['kammwi01', '1923'] = \n", r1)

    new_values = {"stint":"5"}
    print("\nUpdating ['kammwi01', '1923'] with {'stint':'5'}:")
    affected_num = data_table.update_by_key(key_fields=key_fields, new_values=new_values)
    print("Number of Rows updated = \n", affected_num)

    r2 = data_table.find_by_primary_key(key_fields)
    print("Details for ['kammwi01', '1923'] after update = \n", r2)

    print("******************** " + "end update_by_key_test" + " ********************")

def update_by_template_test(data_table, template=None, new_values=None):
    print("\n\n")
    print("******************** " + "update_by_template_test" + " ********************")

    insertedRecord1 = {"playerID":"Hope", "yearID":"1997", "stint":"1", "teamID":"Columbia",
                 "lgID":"NN", "G":"9", "AB":"8", "R":"7","H":"6", "2B":"5", "3B":"4",
                 "HR":"3", "RBI":"2", "SB":"1","CS":"0", "BB":"0", "SO":"0", "IBB":"0",
                 "HBP":"0", "SH":"0", "SF":"0", "GIDP":"0"}
    data_table.insert(insertedRecord1)

    insertedRecord2 = {"playerID":"Hope2", "yearID":"1997", "stint":"1", "teamID":"Columbia",
                 "lgID":"NN", "G":"9", "AB":"8", "R":"7","H":"6", "2B":"5", "3B":"4",
                 "HR":"3", "RBI":"2", "SB":"1","CS":"0", "BB":"0", "SO":"0", "IBB":"0",
                 "HBP":"0", "SH":"0", "SF":"0", "GIDP":"0"}
    data_table.insert(insertedRecord2)

    template = {"teamID":"Columbia"}
    r1 = data_table.find_by_template(template=template)
    print("Details for {'teamID':'Columbia'} = \n", json.dumps(r1, indent=2))

    new_values = {"stint":"2"}
    print("\nUpdating to {'stint':'2'}:")
    affected_num = data_table.update_by_template(template=template, new_values=new_values)
    print("Number of Rows updated = \n", affected_num)

    r2 = data_table.find_by_template(template)
    print("Details after update = \n", json.dumps(r2, indent=2))

    print("******************** " + "end update_by_template_test" + " ********************")

def insert_test(data_table, new_record=None):
    print("\n\n")
    print("******************** " + "insert_test" + " ********************")

    new_record = {"playerID":"Hope", "yearID":"1997", "stint":"1", "teamID":"Columbia",
                 "lgID":"NN", "G":"9", "AB":"8", "R":"7","H":"6", "2B":"5", "3B":"4",
                 "HR":"3", "RBI":"2", "SB":"1","CS":"0", "BB":"0", "SO":"0", "IBB":"0",
                 "HBP":"0", "SH":"0", "SF":"0", "GIDP":"0"}
    print("Inserting new record: ", json.dumps(new_record, indent=2))
    data_table.insert(new_record)

    print("Insertion result:")
    print(data_table.find_by_primary_key(["Hope", "1997", "1", "Columbia"]))

    print("******************** " + "end insert_test" + " ********************")


if __name__ == '__main__':
    data_table_t = init_data_table_test()
    find_by_primary_key_test(data_table=data_table_t)
    find_by_template_test(data_table=data_table_t)
    delete_by_key_test(data_table=data_table_t)
    delete_by_template_test(data_table=data_table_t)
    update_by_key_test(data_table=data_table_t)
    update_by_template_test(data_table=data_table_t)
    insert_test(data_table=data_table_t)

    print("Done!")
