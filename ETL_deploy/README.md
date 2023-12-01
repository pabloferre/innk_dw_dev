#Inicializar el servicio

Para iniciar, 
sudo systemctl start etl.service

Para detenerlo, 
sudo systemctl stop etl.service

Para reiniciarlo
sudo systemctl restart etl.service

Para saber si levantó
sudo systemctl status etl.service

Para ver los logs, 
journalctl -u etl.service -n500 -b -f


#Para inicar desde docker los containers

#Si es primera vez que se hace el build, se debe otorgar permisos a de acceso a la carpeta

sudo cmod 777 /home/innk_dw/innk_dw_dev/ETL_deploy/



#Problemas frecuentes

Tareas en estado "qeued": Si el servidor se cae, se deben bajar y subir los containers. Es posible que el worker de Airflow haya quedado colgado y cuando se reinicialice de un error tipo: "ERROR: Pidfile (/usr/local/airflow/airflow-worker.pid) already exists." lo que deriva en que las tareas quedan en estado "queued". Para eso se debe eliminar el archivo innk_dw/innk_dw_dev/ETL_deploy/airflow-worker.pid. Si se está en el directorio de ejecución de Docker basta con:

sudo rm -rf airflow-worker.pid