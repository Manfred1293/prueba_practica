import json
import numpy as np
import mysql.connector
import hmac
import hashlib
import base64
                
#Inicio del codigo del endpoint que recibe la infromacion del webhook de shopify
from datetime import datetime
from flask import Flask, request, abort

app = Flask(__name__)


#Endpoint that process the payload of the webhook
@app.route('/', methods=['POST'])
def webhook():
                if request.method == 'POST':
                    print(request.json["id"])
                    print(request.json)
                    shopify_webhook_event = str(request.headers.get("X-Shopify-topic"))
                    print("PRINT SHOPIFY EVENT: " + str(request.headers.get("X-Shopify-topic")))
                    
                    if str(request.headers.get("X-Shopify-topic")) == "orders/fulfilled":
                        print ("************************ORDER PAYMENT WEBHOOK*******************")
                        req_data = request.get_json(force = True)
                        #print(req_data)
                        respuesta = processRequest(req_data,shopify_webhook_event)
                        r = respuesta
                    else:
                        if str(request.headers.get("X-Shopify-topic")) == "products/create" :
                            print ("************************PRODUCT CREATION WEBHOOK*******************")
                            req_data = request.get_json(force = True)
                            respuesta = processRequest(req_data,shopify_webhook_event)
                            r = respuesta
                        else:
                            if str(request.headers.get("X-Shopify-topic")) == "products/update" :
                                    print ("************************PRODUCT UPDATE WEBHOOK*******************")
                                    req_data = request.get_json(force = True)
                                    #print(req_data)
                                    respuesta = processRequest(req_data, shopify_webhook_event)
                                    r = respuesta
                            else:
                                  if str(request.headers.get("X-Shopify-topic")) == "products/delete" :
                                        print ("************************PRODUCT DELETE WEBHOOK*******************")
                                        print(request.json)     
                                        req_data = request.get_json(force = True)
                                        respuesta = processRequest(req_data, shopify_webhook_event)
                                        r = respuesta
                                  else:
                                           r = 'FAIL', 400,
                else:
                    abort(400)
                return r

def processRequest (req_data, shopify_webhook_event):

                if str(request.headers.get("X-Shopify-topic")) == "orders/fulfilled":
                    #print ("************************ORDER WITH THE PRODUCTS IN STRING FORMAT*******************")
                    convert_lin_prod = str(json.dumps(req_data["line_items"]))
                    convert_billing_address = str(json.dumps(req_data["billing_address"]))

                    header_bill_id = str(req_data["id"])
                    header_bill_num_order = str(req_data["number"])
                    header_bill_total_amount = str(req_data["total_price"])
                    header_bill_date_creation = str(req_data["created_at"])
                    header_bill_tipo_moneda = str(req_data["currency"])

                    #print ("************************PRODUCTS EXTRACT AND CONVERTED TO A JSON FORMAT*******************")
                    json_prod = json.loads(convert_lin_prod)
                    json_bill_add = json.loads(convert_billing_address)
                    print(json_bill_add)
                    
                    client_first_nsme = str(json_bill_add["first_name"])
                    client_phone = str(json_bill_add["phone"])
                    client_email = str(req_data["email"])
                    client_address1 = str(json_bill_add["address1"])
                    
                    
                    for single_product in json_prod:
                        print ("\n********************INFORMATION EXTRACT OF EACH PRODUCT*******************")
                        #CREATE A TEMPORARY STRING WITH ALL THE TAGS OF THE JSON PRODUCT THATY CONTAINS THE JSON FILE OF THE PRODUCTS
                        temporary_string = str(json.dumps(single_product))
                        json_temporary_string = json.loads(temporary_string)
                        print(single_product)
                        
                        #THIS SECTION PULL THE DIFERENT DATA NECESARY FOR THE UPDATE IN MYSQL DATABASE  
                        body_bill_id = str(single_product["product_id"])
                        body_bill_img_url = str(single_product["admin_graphql_api_id"])
                        body_bill_sku = str(single_product["sku"])
                        body_bill_name = str(single_product["name"])
                        body_bill_quantity = str(single_product["quantity"])
                        body_bill_price = str(single_product["price"])
                        body_bill_total_price = str(req_data["total_price"])
                        client_id = str(header_bill_id)+str(body_bill_id)

                        #THIS SECTION PRINTS THE VALUES TO CONFIRM THE INFORMATION
                        print ("\n************************VALUES USE FOR THE QUERY CONSULT*******************")
                        print("PRODUCT ID:"+ str(single_product["product_id"]))
                        print("PRODUCT QUANTITY:"+ str(single_product["quantity"]))

                        #THIS SET THE ARRAY WITH THE INFO TO UPDTAE THE DATA BASE
                        update_values_db = np.array([header_bill_id, header_bill_num_order, header_bill_total_amount, header_bill_date_creation, header_bill_tipo_moneda,
                                                     body_bill_id, body_bill_img_url, body_bill_sku, body_bill_name, body_bill_quantity, body_bill_price, body_bill_total_price,
                                                     client_id, client_first_nsme, client_phone, client_email, client_address1])

                        #THIS CALL THE FUNTION TO ACCESS THE DATA BASE AND UPDATE THE VALUES DEPENDING OF THE WEBHOOK THAT WE RECEIVE
                        data_base_updated = update_data_base(update_values_db,shopify_webhook_event )
                        
                        print ("\n************************THIS IS THE JSON PAYLOAD OF EACH SINGLE PRODUCT*******************")
                        print(single_product)
                        

                    print ("************************RESPONCE TO THE WEBHOOK 200 OK*******************")
                    respuesta = 'success', 200,
                else:

                        #************************BEGGINS PRODUCT CREATION SECTION OF THE FUNCTION******************************


                        if str(shopify_webhook_event) == "products/create" :
                            #print("************************VALUES FROM OBJECT PRODUCT TO BE ADD IN THE DB*******************")
                            #PULL THE DATA NECESARY TO FOR THE INSERT QUERY AND SAVE IT IN THE VARIABLES FOR FUTHER USE
                            temporary_req_id =  str(req_data["id"])
                            temporary_str_nombre =  str(req_data["title"])

                            now = datetime.now()
                            temporary_fech_regfistro = now.strftime("%Y-%m-%d")                                        
                            temporary_ult_fech_regfistro =  now.strftime("%Y-%m-%d")
                            
                            print ("************************PRINTING VARIANTS JSON*******************")
                            convert_lin_prod = str(json.dumps(req_data["variants"]))
                            json_prod = json.loads(convert_lin_prod)
                            for single_product in json_prod:
                                temporary_string = str(json.dumps(single_product))
                                json_temporary_string = json.loads(temporary_string)

                                #PULL THE DATA NECESARY TO FOR THE INSERT QUERY 
                                temporary_str_id = str(json_temporary_string["product_id"])
                                temporary_str_img_url =  str(json_temporary_string["admin_graphql_api_id"])
                                temporary_str_quantity =  str(json_temporary_string["inventory_quantity"])
                                temporary_str_sku =  str(json_temporary_string["sku"])

                                #PRINT ALL THE VARIABLES LOADED UNTIL THIS POINT FOR VISUAL VERIFICATION
                                print (temporary_req_id, temporary_str_id, temporary_str_sku, temporary_str_img_url,temporary_str_nombre,
                                       temporary_str_quantity, temporary_fech_regfistro, temporary_ult_fech_regfistro)

                                #LOAD ALL THE VARIABLES IN THE ARRAY update_values_db TO PASS THE ARRAY TO THE FUNTION update_data_base 
                                update_values_db = np.array([temporary_req_id, temporary_str_id, temporary_str_sku, temporary_str_img_url,
                                                             temporary_str_nombre, temporary_str_quantity, temporary_fech_regfistro,
                                                             temporary_ult_fech_regfistro, temporary_string])
                                #RETUR/CALL OF THE FUNTION update_data_base
                                data_base_updated = update_data_base(update_values_db,shopify_webhook_event)
                            
                            print ("************************RESPONCE TO THE WEBHOOK 200 OK*******************")

                            respuesta = 'success', 200,
                        else:
                            if str(request.headers.get("X-Shopify-topic")) == "products/update" :
                                        temporary_req_id =  str(req_data["id"])
                                        temporary_str_nombre =  str(req_data["title"])

                                        now = datetime.now()
                                        temporary_fech_regfistro = now.strftime("%Y-%m-%d")                                        
                                        temporary_ult_fech_regfistro =  now.strftime("%Y-%m-%d")

                                        convert_lin_prod = str(json.dumps(req_data["variants"]))
                                        json_prod = json.loads(convert_lin_prod)
                                        for single_product in json_prod:
                                                temporary_string = str(json.dumps(single_product))
                                                json_temporary_string = json.loads(temporary_string)

                                                #PULL THE DATA NECESARY TO FOR THE INSERT QUERY 
                                                temporary_str_id = str(json_temporary_string["product_id"])
                                                temporary_str_img_url =  str(json_temporary_string["admin_graphql_api_id"])
                                                temporary_str_quantity =  str(json_temporary_string["inventory_quantity"])
                                                temporary_str_sku =  str(json_temporary_string["sku"])

                                                #PRINT ALL THE VARIABLES LOADED UNTIL THIS POINT FOR VISUAL VERIFICATION
                                                print (temporary_req_id, temporary_str_id, temporary_str_sku, temporary_str_img_url,temporary_str_nombre,
                                                       temporary_str_quantity, temporary_fech_regfistro, temporary_ult_fech_regfistro)

                                                #LOAD ALL THE VARIABLES IN THE ARRAY update_values_db TO PASS THE ARRAY TO THE FUNTION update_data_base 
                                                update_values_db = np.array([temporary_req_id, temporary_str_id, temporary_str_sku, temporary_str_img_url,
                                                                             temporary_str_nombre, temporary_str_quantity, temporary_fech_regfistro,
                                                                             temporary_ult_fech_regfistro, temporary_string])
                                                #RETUR/CALL OF THE FUNTION update_data_base
                                                data_base_updated = update_data_base(update_values_db,shopify_webhook_event)
                            
                                        print ("************************RESPONCE TO THE WEBHOOK 200 OK*******************")

                                        respuesta = 'success', 200,
                            else:
                                    if str(request.headers.get("X-Shopify-topic")) == "products/delete" :
                                        convert_lin_prod = str(json.dumps(req_data))
                                        json_prod = json.loads(convert_lin_prod)
                                        update_values_db = json_prod["id"]
                                        data_base_updated = update_data_base(update_values_db,shopify_webhook_event)
                                        data_base_updated = update_data_base(update_values_db,shopify_webhook_event)
                                        respuesta = 'success', 200,
                                    else:
                                        respuesta = 'FAIL', 400,
                return respuesta

def update_data_base (update_values_db,shopify_webhook_event):
        #coneccion a la base de datos local 
          if str(shopify_webhook_event) == "products/create" :
                    sql_select_Query = "SELECT * FROM catalogo_articulos WHERE id = " + str(update_values_db[1])+";"
                    print_records = execute_query(sql_select_Query)
                    #log_inserted_in_db =  insert_log_producto(update_values_db,shopify_webhook_event)
                    
                    print("TOTAL PRODUCTS FOUND: ", str(len(print_records)))

                    if str(len(print_records)) == "0":
                        sql_select_Query = "INSERT INTO catalogo_articulos VALUES"+"(\""+str(update_values_db[1])+"\",\""+str(update_values_db[2])+"\",\""+str(update_values_db[3])+"\",\""+str(update_values_db[4])+"\","+str(update_values_db[5])+"\"); COMMIT;"
                        print_records = execute_query(sql_select_Query)
                        print("RECORD INSERT IN DB")
                        log_inserted_in_db =  insert_log_producto(update_values_db, shopify_webhook_event)
                        
                    else:
                            log_inserted_in_db =  insert_log_producto(update_values_db, shopify_webhook_event)
                            print("\nTABLE INFORMATION: ")
                            for row in print_records:
                                    print("ID = ", row[0] )
                                    print("SKU = ", row[1])
                                    print("Imagenurl = ", row[2])
                                    print("Nombre  = ", row[3])
                                    print("Cantidad  = ", row[4])
                                    print("FechaRegistro  = ", row[5])
                                    print("UltimaFechaActualizada  = ", row[6])
                                    print("Sincronizado  = ", row[7])

                            data_base_updated = len(print_records)
                                    
          else:
                    if str(request.headers.get("X-Shopify-topic")) == "orders/fulfilled":
                        for row in update_values_db:
                            count = count + 1
                            print(update_values_db[count])
                        print ("\n************************CONSULTA A BD TO CHECK FOR EXISTING PRODUCT*******************")
                        sql_select_Query = "SELECT * FROM facturacion_encabezado WHERE id = " + str(update_values_db[0])
                        print_records = execute_query(sql_select_Query)
                        print("QUERY CONSULT: " + str(sql_select_Query))
                        print("TOTAL PRODUCTS FOUND: ", len(print_records))
                        print("PRODUCTS FOUND: ", str(print_records))

                        if str(len(print_records)) == "0":
                            sql_select_Query = "INSERT INTO facturacion_encabezado VALUES"+"(\""+str(update_values_db[0])+"\",\""+str(update_values_db[1])+"\","+str(update_values_db[2])+",\""+str(update_values_db[3])+"\",\""+str(update_values_db[4])+"\",\""+str(update_values_db[3])+"\"); COMMIT;"
                            #print("\nNO PRODUCT FOUND IN THE TABLE")
                            print_records = execute_query(sql_select_Query)

                            sql_select_Query = "SELECT * FROM facturacion_encabezado WHERE id = " + str(update_values_db[0])
                            print_records = execute_query(sql_select_Query)
                            if str(len(print_records)) == "0":
                                print("FAIL TO UPDATE THE DB")
                            else:
                                sql_select_Query = "INSERT INTO facturacion_detalle VALUES"+"(\""+str(update_values_db[0])+"\",\""+str(update_values_db[6])+"\",\""+str(update_values_db[7])+"\",\""+str(update_values_db[8])+"\","+str(update_values_db[9])+",\""+str(update_values_db[10])+"\",\""+str(update_values_db[11])+"\",\""+str(update_values_db[12])+"\"); COMMIT;"
                                print_records = execute_query(sql_select_Query)
                                sql_select_Query = "INSERT INTO facturacion_cliente VALUES"+"(\""+str(update_values_db[0])+"\",\""+str(update_values_db[13])+"\",\""+str(update_values_db[14])+"\",\""+str(update_values_db[15])+"\","+str(update_values_db[16])+",\""+str(update_values_db[17])+"\"); COMMIT;"
                                print_records = execute_query(sql_select_Query)
                                
                            data_base_updated = len(print_records)
                        else:
                              #(ID, SKU, Imagenurl, Nombre, Cantidad, FechaRegistro, UltimaFechaActualizacion, Sincronizado)
                              print("\nTABLE INFORMATION: ")
                              for row in print_records:
                                    print("ID = ", row[0] )
                                    print("SKU = ", row[1])
                                    print("Imagenurl = ", row[2])
                                    print("Nombre  = ", row[3])
                                    print("Cantidad  = ", row[4])
                                    print("FechaRegistro  = ", row[5])
                                    print("UltimaFechaActualizada  = ", row[6])
                                    print("Sincronizado  = ", row[7])
                                    
                              Inventory_quantity = row[4]
                           
                              if str(Inventory_quantity) ==  "0":
                                          print("INVENTARY IS DEPLETED")
                                          ata_base_updated = cursor.rowcount
                                  
                              else:
                                     Inventory_quantity = Inventory_quantity - int(update_values_db[2])
                                     print("INVENTORY QUANTITY" + str(Inventory_quantity))
                                     sql_select_Query = "UPDATE catalogo_articulos SET Cantidad = \""+str(Inventory_quantity)+"\" WHERE ID = \""+str(update_values_db[1])+"\"; COMMIT;"
                                     print_records = execute_query(sql_select_Query)

                                     #UPDATE THE HEADER OF THE BILL ORDER
                                     
                                     #(ID, SKU, Imagenurl, Nombre, Cantidad, FechaRegistro, UltimaFechaActualizacion, Sincronizado)
                                     print("FINAL QUERY: "+ str(sql_select_Query))
                                     data_base_updated = len(print_records)
                                
                    else:
                              if str(request.headers.get("X-Shopify-topic")) == "products/delete":
                                   
                                   sql_select_Query = "SELECT * FROM catalogo_articulos WHERE id = " + str(update_values_db)
                                   print_records = execute_query(sql_select_Query)
                                   #log_inserted_in_db =  insert_log_producto(update_values_db, shopify_webhook_event)
                                   
                                   if str(len(print_records)) == "0":
                                           print("\nNO PRODUCT FOUND IN THE TABLE")
                                           data_base_updated = len(print_records)
                                   else:
                                          sql_select_Query = "DELETE FROM catalogo_logarticulos WHERE IDArticulo =\""+str(update_values_db)+"\"; COMMIT;"
                                          query_records = execute_query(sql_select_Query)
                                          sql_select_Query = "DELETE FROM catalogo_articulos WHERE id =\""+str(update_values_db)+"\"; COMMIT;"
                                          #(ID, SKU, Imagenurl, Nombre, Cantidad, FechaRegistro, UltimaFechaActualizacion, Sincronizado)
                                          print("DELETE QUERY EXECUTE")
                                          query_records = execute_query(sql_select_Query)
                                          log_inserted_in_db =  insert_log_producto(update_values_db, shopify_webhook_event)
                                          data_base_updated = len(print_records)
                                          print("TOTAL PRODUCTS FOUND: ", len(print_records))
                              else:
                                    if str(request.headers.get("X-Shopify-topic")) == "products/update" :
                                          sql_select_Query = "SELECT * FROM catalogo_articulos WHERE id = " + str(update_values_db[0])
                                          print_records = execute_query(sql_select_Query)
                                          #log_inserted_in_db =  insert_log_producto(update_values_db, shopify_webhook_event)
                                          
                                          if str(len(print_records)) == "0":
                                                 sql_select_Query = "INSERT INTO catalogo_articulos VALUES"+"(\""+str(update_values_db[1])+"\",\""+str(update_values_db[2])+"\",\""+str(update_values_db[3])+"\",\""+str(update_values_db[4])+"\","+str(update_values_db[5])+",\""+str(update_values_db[6])+"\",\""+str(update_values_db[7])+"\",\"1\"); COMMIT;"
                                                 print_records = execute_query(sql_select_Query)
                                                 log_inserted_in_db =  insert_log_producto(update_values_db, shopify_webhook_event)
                                                 print("RECORD INSERT IN DB")
                                          else:
                                                 sql_select_Query = "UPDATE catalogo_articulos SET ID = \""+str(update_values_db[1])+"\", SKU = \""+str(update_values_db[2])+"\", Imagenurl = \""+str(update_values_db[3])+"\", Nombre = \""+str(update_values_db[4])+"\", Cantidad = "+str(update_values_db[5])+", FechaRegistro = \""+str(update_values_db[6])+"\", UltimaFechaActualizacion = \""+str(update_values_db[7])+"\", Sincronizado = \"1\" WHERE ID ="+str(update_values_db[1])+"; COMMIT;"
                                                 #(ID, SKU, Imagenurl, Nombre, Cantidad
                                                 print("FINAL QUERY: "+ str(sql_select_Query))
                                                 print_records = execute_query(sql_select_Query)
                                                 log_inserted_in_db =  insert_log_producto(update_values_db, shopify_webhook_event)
                                                
def insert_log_producto(update_values_db, shopify_webhook_event):
                 now = datetime.now()
                 temporary_key_log = now.strftime("%Y%m%d%H%M%S")

                 if str(request.headers.get("X-Shopify-topic")) == "products/delete":
                         log_inserted_in_db = "DELETE PRODUCT NO LOG REGISTRED"
                 else:
                         temporary_update_values_db = str(update_values_db[0])
                         temporary_key = str(temporary_update_values_db)+str(temporary_key_log)
                 
                         sql_select_Query = "SELECT * FROM catalogo_logarticulos WHERE id = " + str(temporary_key)
                         print_records = execute_query(sql_select_Query)
                         print(str(len(print_records)))

                         if str(len(print_records)) == "0":
                               print(str(temporary_update_values_db))
                               sql_select_Query = "INSERT INTO catalogo_logarticulos VALUES"+"(\""+str(temporary_key)+"\",\""+update_values_db[1]+"\",\""+str(update_values_db[0])+"\",\""+str(update_values_db[7])+"\"); COMMIT;"
                               print(sql_select_Query)
                               print_records = execute_query(sql_select_Query)
                               log_inserted_in_db = "Catalogo_Log Updated "
                         
                         else:
                               log_inserted_in_db = "CATALOGO_LOG ALREADFY REGISTER"
                               print(log_inserted_in_db)
                               
                 return log_inserted_in_db
                 #(ID, SKU, Imagenurl, Nombre, Cantidad, FechaRegistro, UltimaFechaActualizacion, Sincronizado)
                        

def execute_query(sql_select_Query):
                try:
                        connection = mysql.connector.connect(host="localhost",
                        database="prueba_practica_ar_holdings",
                        user="root",
                        passwd="Manny12031993-")
                        cursor = connection.cursor()
                        print(sql_select_Query)
                        cursor.execute(sql_select_Query)
                        print("QUERY EXECUTED")
                        records = cursor.fetchall()
                        print_records = records
                        #print(str(print_records))
                        
                except mysql.connector.Error as e:
                            print("Error reading data from MySQL table", e)
                finally:
                        if connection.is_connected():
                            connection.close()
                            cursor.close()
                            print("MySQL CONNECTION CLOSE.....")

                return  print_records


        
if __name__ == '__main__':
    app.run()
    
