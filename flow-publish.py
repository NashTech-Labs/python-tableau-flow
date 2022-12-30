from getpass import getpass, getuser
from pathlib import Path
import tableauserverclient as TSC
import polling2
import os

CONN_PASSWD=os.getenv('CONN_PASSWD')

tableau_auth = TSC.PersonalAccessTokenAuth('{}'.format(os.getenv('TOKEN_NAME')), '{}'.format(os.getenv('TOKEN')), '{}'.format(os.getenv('TABLEAU_SITE')))

server = TSC.Server('{}'.format(os.getenv('HOST')),use_server_version=True)

flow_file ="{}".format(os.getenv('TABLEAU_FLOW_FILE_PATH'))

connection_credentials = TSC.ConnectionCredentials(
            '{}'.format(os.getenv('CONN_USER')),
            CONN_PASSWD,
            embed=True,
            oauth=True
)

with server.auth.sign_in(tableau_auth):

    project = server.projects.filter(name="Default")[0]
    print("projectName : {}\n projectID:{}".format(project.name, project.id))

    #flow = TSC.models.flow_item.FlowItem(project_id=project.id, "TestFlow Sachin")
    flow = TSC.models.flow_item.FlowItem(project_id=project.id)

    output_flow_item=server.flows.publish(
        flow,
        file_path=str(flow_file),
        mode=server.PublishMode.Overwrite
    )

    print("FLOW:{}".format(output_flow_item))
    print("output_flow_item From Flow publish method \n flowName:{}\nflowId:{}".format(output_flow_item.name,output_flow_item.id))

    print("Get connection item")

    server.flows.populate_connections(output_flow_item)
    print("connection info :{}".format(output_flow_item.connections))

    for  connection_item in output_flow_item.connections:

        print("Connection Info: \nDS:{} \nUsername:{}\npassword:{}".format(connection_item.datasource_name, connection_item.username, connection_item.password))
        if connection_item.connection_type == "snowflake":
            #connection_item.connection_credentials = CONN_PASSWD
            # connection_item.password = CONN_PASSWD
            
            connection_item.embed_password = True
            connection_item.username = '{}'.format(os.getenv('CONN_USER'))
            connection_item.password = CONN_PASSWD       
            connection_item_updated=server.flows.update_connection(output_flow_item,connection_item)     

        print("Connection Info: \nDS:{}  \nUsername:{}\npassword:{}".format(connection_item.datasource_name,connection_item.username, connection_item.password))

    print("refreshing the flow \n\t{}:{}".format(output_flow_item.name,output_flow_item.id))


    job = server.flows.refresh(output_flow_item)

    # job completion status types
    job_status = ["Success", "Failed", "Cancelled"]
    # loop until job is in pending/in progress status
    polling2.poll(lambda: server.jobs.get_by_id(job.id).finish_code != -1, step=30, poll_forever=True)
    # print the completion status
    print('Job finished with status: ' + job_status[int(server.jobs.get_by_id(job.id).finish_code)])    


