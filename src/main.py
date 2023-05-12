#!/usr/bin/env python
#_*_ codig: utf8 _*_
import os, sys, traceback, datetime, time, json
import boto3
from Modules.functions import * 
from Modules.Constants import *

if __name__ == '__main__':
#---Se inicia el contador de files subidos en 0
    Counter=0
    Counter_videos=0
    Counter_imagenes=0
    Counter_xmls=0
    dict_sumary={}
    list_packs_inconsistancies=[]
#---Inicio del cliclo infinito    
    while True:
        try:
#-----------Captura de la fecha y hora del sistema en formato texto            
            date_log=str(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
#-----------Dato de fecha            
            Date=date_log.split(' ')[0]
#-----------Dato de hora            
            Hour=date_log.split(' ')[1]
#-----------Texto a imprimir en el archivo log            
            text_print='Init'
#-----------Impresion del texto en el archivo log mediante la funcion personalizada print_log()            
#            print_log("a",text_print, Date)
#-----------Se estable el perfil a utilizar para crear conexion con aws
            aws_session=boto3.Session(profile_name='pythonapps')
#-----------Se establece conexion con el servicio S3 de AWS
            s3=aws_session.client('s3')
#-----------Variable que registra la cuenta de files al comenzar el siguinete ciclo
            Counter_Before=Counter
#-----------Lista lista de folder correspondientes a los canales            
            folders_Channels=os.listdir(source_Path)
#-----------Impresion en el archivo log cuantos canales componen la lista            
#            print_log("a", f"Channels List {len(folders_Channels)}\t{folders_Channels}", Date)
            #print(f"Channels List {len(folders_Channels)} {folders_Channels}")
#-----------Se recorre la lista de canales uno por uno            
            for channel in folders_Channels:
#---------------Se imprime en pantalla el canal selecionado actualmente
                print(f'Channel {channel} Selected')
#---------------Seleccion del bucket segun canal seleccionado altualmente
                Bucket=Channels[channel]
#---------------Lista de paquetes VOD del canal actual
                VOD_Packages=os.listdir(f"{source_Path}/{channel}")
#---------------Pregunta si la lista de de paquetes es vacia
                if VOD_Packages == []:
#-------------------Si la lista de paquetes es vacia se imprime el estado en el log
#                    print_log("a", f"Empty {channel} folder", Date)
                    #print(f"Empty {channel} folder")
                    pass
                else:
#-------------------Se imprime en pantalla el numero de paquetes encontrados, la lista de los paquetes y el canal selecionado
                    print(f'VOD_Packages list {channel} {len(VOD_Packages)} {VOD_Packages}')
#-------------------lista de los files correspondientes al paquete seleccionado
                    for VOD_Pack in VOD_Packages:
                        print(f'{channel} VOD_Pack {VOD_Pack} Selected')  
#-----------------------lista de files dentro del paquete
                        Files=os.listdir(f"{source_Path}/{channel}/{VOD_Pack}")
                        print(f'File list {len(Files)} {Files}')
#-----------------------Se organizan los files con la funcion personalizada Organize() en el orden video, imagenes y xml
                        Files=Organize(Files)
#-----------------------Se imprime en el log el resuldado de la lista organizada
                        print(f'File list organized {Files}')
#-----------------------Recorre la lista de files uno a uno
                        for File in Files:
#---------------------------Se imprime en el log el file selecionado
                            print(f'File {File} Selected')
#---------------------------Se establece la ruta del file selecionado
                            file_Path=f"{source_Path}/{channel}/{VOD_Pack}/{File}"
#---------------------------Pregunta si el file es un xml
                            if VOD_Pack+'.xml' == File:
                                print('XML Found')
#-------------------------------Pregunta si el file existe en la base de datos con la funcion personalizada DB_Verify 
                                if s3.list_objects_v2(Bucket=Bucket, Prefix=f"{channel}/{VOD_Pack}/{File}")['KeyCount'] != 0:
#-----------------------------------Si el archivo existe se en listan todos los archivos del paquete                                    
                                    List_Del_Files=os.listdir(f"{source_Path}/{channel}/{VOD_Pack}")
#-----------------------------------Se recore la lista de files
                                    for Del_File in List_Del_Files:
#---------------------------------------Se establece la ruta del file selecionado
                                        file_Delete_Path=f"{source_Path}/{channel}/{VOD_Pack}/{Del_File}"
#---------------------------------------Se elimina el file selecionado
                                        os.remove(file_Delete_Path)
#---------------------------------------Se imprime en el log la ruta del file eliminado
                                        print(f"File Deleted {file_Delete_Path}")
#-----------------------------------Se elimina el folder del paquete una vez esta vacio 
                                    os.rmdir(f"{source_Path}/{channel}/{VOD_Pack}")
#-----------------------------------Se sale del cilco for actual que recorre los files del paquete selecionado
                                    break    
                                else:
#-----------------------------------Si el xml no existe en la base de datos se extraen los datos del mismo con la funcion personalizada readXML 
                                    Dictionary=readXML(file_Path)
#-----------------------------------Se establece la cantidad de files correspondientes al paquete selecionado segun el xml
                                    CantXML=len(Dictionary)
#-----------------------------------Se establece el contador de files en la base de datos correspondiente al paquete seleccionado en 0
                                    CantDB=0
#-----------------------------------Se secorre la lista de files segun los datos extraidos del xml
                                    for Contend in Dictionary:
#---------------------------------------Preguta si el file seleccionado de la lista corresponde a un video
                                        if Contend == 'video':
#-------------------------------------------Pregunta si existe el file de video especificado en el xml en la nube
                                            if s3.list_objects_v2(Bucket=Bucket, Prefix=f"{channel}/{VOD_Pack}/{Dictionary['video']['name']}")['KeyCount'] != 0:
#-----------------------------------------------Si existe quiere decir que el archivo ya esta en nube y se aumenta el contador en 1
                                                CantDB+=1
                                            else:
#-----------------------------------------------Si no existe se sale del ciclo for que recorre la lista de files segun el xml
                                                break
#---------------------------------------Pregunta si el file seleccionado actualmente corresponde a una imagen
                                        elif s3.list_objects_v2(Bucket=Bucket, Prefix=f"{channel}/{VOD_Pack}/{Dictionary[Contend]}")['KeyCount'] != 0:
#-------------------------------------------Si existe registro se aumenta el contador en 1
                                            CantDB+=1
#---------------------------------------Si el file no es video o imagen 
                                        else:
#-------------------------------------------Valida si elcontador de files subidos es diferente de cero y si en el cliclo actual de while no se ha subido ningun otro file
                                            if Counter!=0 and Counter==Counter_Before:
#-----------------------------------------------Se crea archivo para adjuntar al email
                                                list_packs_inconsistancies.append(VOD_Pack)
                                                dict_sumary['inconsistencies_packs']=list_packs_inconsistancies
                                                print(f"{VOD_Pack} Are finded inconsistancies in this package")
#-------------------------------------------Se sale del actual ciclo for 
                                            break
#-----------------------------------Pregunta si la cantidad de files registrados en la base de datos es igual a la cantidad de files extraidos del xml
                                    if CantDB==CantXML:
#---------------------------------------Se establece la ruta en ek bucket de subir el file
                                        object_name=f"{channel}/{VOD_Pack}/{File}"
                                        try:
#-------------------------------------------Se utiliza el metodo para subir el archivo al bucket
                                            s3.upload_file(file_Path, Bucket, object_name, Config=boto3.s3.transfer.TransferConfig(max_bandwidth=30000000))
#-------------------------------------------Captura del dato de fecha y hora de la subida del archivo
                                            Counter_xmls+=1
                                            Counter+=1
                                        except:
#-------------------------------------------Captura del error del sistema
                                            error=sys.exc_info()[2]
                                            error_Info=traceback.format_tb(error)[0]
#-------------------------------------------Se imprime en el log cuando un error de ejecucion se presenta
                                            print(f'An error occurred while uploading file {object_name} to bucket ')
                                            continue
#-----------------------------------Si no se cumplen las validaciones anteriores continua con la siguiente linea
                                    else:
                                        pass
#---------------------------Valida si el file es un video .mp4
                            elif VOD_Pack+'.mp4' == File:
#-------------------------------Verifica si hay registro del file en la base de datos
                                if s3.list_objects_v2(Bucket=Bucket, Prefix=f"{channel}/{VOD_Pack}/{File}")['KeyCount'] != 0:
#-----------------------------------pasa a la siguiente linea sin subirlo a bucket
                                    pass
                                else:
#-----------------------------------Se establece la ruta para subir el file al bucket
                                    object_name=f"{channel}/{VOD_Pack}/{File}"
                                    try:
#---------------------------------------Se utiliza el metodo para subir el archivo al bucket
                                        s3.upload_file(file_Path, Bucket, object_name, Config=boto3.s3.transfer.TransferConfig(max_bandwidth=30000000))
#---------------------------------------Aumenta en 1 el contador de files subidos a S3
                                        Counter_videos+=1
                                        Counter+=1
                                    except:
#---------------------------------------Captura del error que arroja el sistema
                                        error=sys.exc_info()[2]
                                        error_Info=traceback.format_tb(error)[0]
#---------------------------------------Se imprime en pantalla el error generado
                                        print(f'An error occurred while uploading file {object_name} to bucket ')
                                        continue
#---------------------------Pregunta si el mail es una imagen jpg
                            elif '.jpg' in File:
#-------------------------------Verifica en la base de datos si existe registro del file
                                if s3.list_objects_v2(Bucket=Bucket, Prefix=f"{channel}/{VOD_Pack}/{File}")['KeyCount'] != 0:
#-----------------------------------Si hay registro en la base de datos continua sin subir el archivo
                                    pass
                                else:
#-----------------------------------Si no hay registro en la base de datos  se establece la ruta de destino
                                    object_name=f"{channel}/{VOD_Pack}/{File}"
                                    try:
#---------------------------------------Se utiliza el metodo upload para subir el archivo al bucket
                                        s3.upload_file(file_Path, Bucket, object_name, Config=boto3.s3.transfer.TransferConfig(max_bandwidth=30000000))
#---------------------------------------Captura del dato de fecha y hora
                                        date=str(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
#---------------------------------------Aumenta el contador de files subidos a S3 en 1
                                        Counter_imagenes+=1
                                        Counter+=1
                                    except:
#---------------------------------------Captura del error que arroja el sistema
                                        error=sys.exc_info()[2]
                                        error_Info=traceback.format_tb(error)[0]
#---------------------------------------Se imprime en el log el error generado
                                        print(f'An error occurred while uploading file {object_name} to bucket ')
                                        continue
                            else:
#-------------------------------Continua con el ciclo si el file no corresponde a los esperados
                                pass
#-----------Pregunta si el contador es diferente de cero y si el contador es igual al anterior ciclo
            if Counter!=0 and Counter==Counter_Before:
#---------------Cuerpo del correo a enviar
                dict_sumary['Total videos uploaded']=Counter_videos
                dict_sumary['Total Images uploaded']=Counter_imagenes
                dict_sumary['Total Xmls uploaded']=Counter_xmls
                Counter=0
                Counter_videos=0
                Counter_imagenes=0
                Counter_xmls=0
                dict_sumary={}
                list_packs_inconsistancies=[]
#---------------Se envia email de alerta
                SendMail(str(dict_sumary), Subject="awsuploadtoolbox Execution Summary" )
#---------------Regresa el contador de files registrados a cero
                Counter=0
#-----------Pregunta si el contador de files registrados es cero, esto indica que el codigo puede finalizar
            elif Counter==0:
#---------------Se imprime en pantalla que el codigo ha finalizado
                print(Date, "end\n\n")
#---------------Espera 5 minutos para volver a ejecutar el codigo
                time.sleep(300)
            else:
                pass  
        except:
#-----------Captura error del sistema
            error=sys.exc_info()[2]
            error_Info=traceback.format_tb(error)[0]
            dict_sumary['Total videos uploaded']=Counter_videos
            dict_sumary['Total Images uploaded']=Counter_imagenes
            dict_sumary['Total Xmls uploaded']=Counter_xmls
            dict_sumary['Execution Error']={
                'traceback info': error_Info,
                'Error': str(sys.exc_info()[1])
            }
            dict_str_json=json.dumps(dict_sumary, sort_keys=False, indent=8)
            print(dict_sumary)
#-----------Envio del correo de alerta            
            SendMail(str(dict_str_json), Subject="awsuploadtoolbox Ecxecutio Error")
#-----------interumpe la ejecucion del codigo
            quit()
