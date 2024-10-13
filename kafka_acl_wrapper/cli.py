import typer
import config
import acl_bindings
import common_docs
from utility import *


app = typer.Typer(name="kafka-acl-wrapper", pretty_exceptions_show_locals=False)

def populate_acl_binding_options(admin, topic, principal, group, consumer, producer, operations, prefixed):
    acl_binding_options = acl_bindings.acl_binding_options(admin)
    acl_binding_options.set_topic(topic)
    acl_binding_options.set_group(group)
    acl_binding_options.set_principal(principal)
    acl_binding_options.set_prefixed(prefixed)
    acl_binding_options.set_consumer(consumer)
    acl_binding_options.set_producer(producer)
    acl_binding_options.set_operations(kafka_operations_list(operations))
    return acl_binding_options

@app.command()
def get(topic: str = typer.Option("", "--topic", help = common_docs.TOPIC_DESCRIPTION), 
        principal: str = typer.Option("", "--principal", help = common_docs.PRINCIPAL_DESCRIPTION),
        group: str = typer.Option("", "--group", help = common_docs.GROUP_DESCRIPTION),
        consumer: bool = typer.Option(False, "--consumer", help = common_docs.CONSUMER_DESCRIPTION),
        producer: bool = typer.Option(False, "--producer", help = common_docs.PRODUCER_DESCRIPTION),
        operations: str = typer.Option("", "--operations", help = common_docs.OPERATIONS_DESCRIPTION),
        prefixed: bool = typer.Option(False, "--prefixed", help = common_docs.PREFIXED_DESCRIPTION),
        args: str = typer.Argument("")
        ):
    if (topic + principal + group + args == ""):
        print("[red]require at least one argument from --topic / --principal / --group[/red]")
        raise typer.Abort()

    admin = config.setup_kafka_admin_client()
    if args == "all":
        acls = get_all_acl_bindings(admin)
    else:
        acl_binding_options = populate_acl_binding_options(admin, topic, principal, group, consumer, producer, operations, prefixed)
        acls = get_acl_bindings(acl_bind_opts=acl_binding_options)
        if not acls:
            print("[red] no matching acl found [/red]")
        else:
            print(acls)

@app.command()
def create(topic: str = typer.Option("", "--topic", help = common_docs.TOPIC_DESCRIPTION), 
           principal: str = typer.Option("", "--principal", help = common_docs.PRINCIPAL_DESCRIPTION),
           group: str = typer.Option("", "--group", help = common_docs.GROUP_DESCRIPTION),
           consumer: bool = typer.Option(False, "--consumer", help = common_docs.CONSUMER_DESCRIPTION),
           producer: bool = typer.Option(False, "--producer", help = common_docs.PRODUCER_DESCRIPTION),
           operations: str = typer.Option("", "--operations", help = common_docs.OPERATIONS_DESCRIPTION),
           prefixed: bool = typer.Option(False, "--prefixed", help = common_docs.PREFIXED_DESCRIPTION),
           args: str = typer.Argument("")
           ):
    if (topic + principal + group + args == ""):
        print("[red]require at least one argument from --topic / --principal / --group[/red]")
        raise typer.Abort()
    
    admin = config.setup_kafka_admin_client()
    acl_binding_options = populate_acl_binding_options(admin, topic, principal, group, consumer, producer, operations, prefixed)
    create_acl_bindings(acl_bind_opts=acl_binding_options)

@app.command()
def delete(topic: str = typer.Option("", "--topic", help = common_docs.TOPIC_DESCRIPTION), 
           principal: str = typer.Option("", "--principal", help = common_docs.PRINCIPAL_DESCRIPTION),
           group: str = typer.Option("", "--group", help = common_docs.GROUP_DESCRIPTION),
           consumer: bool = typer.Option(False, "--consumer", help = common_docs.CONSUMER_DESCRIPTION),
           producer: bool = typer.Option(False, "--producer", help = common_docs.PRODUCER_DESCRIPTION),
           operations: str = typer.Option("", "--operations", help = common_docs.OPERATIONS_DESCRIPTION),
           prefixed: bool = typer.Option(False, "--prefixed", help = common_docs.PREFIXED_DESCRIPTION),
           args: str = typer.Argument("")
           ):
    if (topic + principal + group + args == ""):
        print("[red]require at least one argument from --topic / --principal / --group[/red]")
        raise typer.Abort()
    
    admin = config.setup_kafka_admin_client()
    acl_binding_options = populate_acl_binding_options(admin, topic, principal, group, consumer, producer, operations, prefixed)
    acl_results = get_acl_bindings(acl_bind_opts=acl_binding_options)
    if not acl_results:
        print("[red] No ACL to be removed [/red]")
        return
    
    print(f" \n \n found acls from filter:\n")
    for acl in acl_results:
        print_acl(acl)
    print("\n \n")
    if typer.confirm(f"are you sure you want to remove these ACLs :"):
        delete_acl_bindings(acl_bind_opts=acl_binding_options)
    else:
        print("[red] canceling delete operations [/red]")

