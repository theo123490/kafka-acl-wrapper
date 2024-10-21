from rich import print
import copy
import confluent_kafka.admin as kafka_admin
import confluent_kafka.error 
import confluent_kafka.error 
import copy
import confluent_kafka.error
import copy
import acl_bindings
import typer
import re

def get_all_acl_bindings(admin: kafka_admin.AdminClient) -> list[kafka_admin.AclBinding]:
    """
    Retrieves all ACL bindings from the Kafka admin client.

    Args:
        admin (kafka_admin.AdminClient): The Kafka admin client.

    Returns:
        list: A list of ACL bindings.
    """
    acl_binding_opts = acl_bindings.acl_binding_options(admin)
    acl_binding_opts.set_all()
    acl_binding_filter_list = create_acl_binding_filter_list(acl_binding_opts)
    acls = get_acl(admin, acl_binding_filter_list[0])
    return acls

def get_acl_bindings(acl_bind_opts: acl_bindings.acl_binding_options):
    """
    Retrieves ACL bindings based on the provided ACL binding options.

    Args:
        acl_bind_opts (acl_bindings.acl_binding_options): The ACL binding options.

    Returns:
        list: A list of ACL bindings.
    """
    acl_binding_filter_list = create_acl_binding_filter_list(acl_bind_opts=acl_bind_opts)
    acls = []
    for acl_filter in acl_binding_filter_list:
        get_acl_result = get_acl(acl_bind_opts.admin, acl_filter)
        if get_acl_result is not None:
            acls.extend(get_acl_result)
    return acls

def delete_acl_bindings(acl_bind_opts: acl_bindings.acl_binding_options):
    """
    Deletes ACL bindings based on the provided ACL binding options.

    Args:
        acl_bind_opts (acl_bindings.acl_binding_options): The ACL binding options.
    """
    acl_binding_filter_list = create_acl_binding_filter_list(acl_bind_opts=acl_bind_opts)
    remove_acl_duplicates(acl_binding_filter_list)
    delete_acl(acl_bind_opts.admin, acl_binding_filter_list)

def create_acl_bindings(acl_bind_opts: acl_bindings.acl_binding_options):
    """
    Creates ACL bindings based on the provided ACL binding options.

    Args:
        acl_bind_opts (acl_bindings.acl_binding_options): The ACL binding options.
    """
    acl_binding_list = create_acl_binding_list(acl_bind_opts=acl_bind_opts)
    
    print(f"creating these acl bindings: \n")
    for acl in acl_binding_list:
        print_acl(acl)

    print("\n \n")

    if typer.confirm("will create these acls, are you sure?"):
        create_acl(acl_bind_opts.admin, acl_binding_list)
    else:
        print(f"[red]cancelling create operation[/red]")

def create_acl_binding_list(acl_bind_opts: acl_bindings.acl_binding_options) -> list[kafka_admin.AclBinding]:
    """
    Creates a list of AclBinding objects based on the provided ACL binding options.

    Args:
        acl_bind_opts (acl_bindings.acl_binding_options): The ACL binding options.

    Returns:
        list[kafka_admin.AclBinding]: A list of AclBinding objects.

    Raises:
        typer.Abort: If specific ACL operations are provided along with the producer or consumer flag, or if the 
        resulting acl_binding_list is empty.
    """
    if len(acl_bind_opts.operations) > 0 and (acl_bind_opts.producer or acl_bind_opts.consumer):
        print("[red]filter with specific acl operations doesn't work with consumer or producer flag[/red]")
        raise typer.Abort()

    acl_binding_list = []
    if acl_bind_opts.prefixed:
        resource_pattern_type = kafka_admin.ResourcePatternType.PREFIXED
    else:
        resource_pattern_type = kafka_admin.ResourcePatternType.LITERAL
    if acl_bind_opts.topic != "":
        topic_acl_template = kafka_admin.AclBinding(
            restype=kafka_admin.ResourceType.TOPIC, 
            name=acl_bind_opts.topic, 
            resource_pattern_type=resource_pattern_type,
            principal=None if acl_bind_opts.principal == "" else acl_bind_opts.principal,
            host="*",
            operation=kafka_admin.AclOperation.UNKNOWN,
            permission_type=kafka_admin.AclPermissionType.ALLOW
        )
        assign_operation_acl(acl_binding_filter_list=acl_binding_list,
                             acl_filter_template=topic_acl_template,
                             producer=acl_bind_opts.producer,
                             consumer=acl_bind_opts.consumer, 
                             operations=acl_bind_opts.operations)

    if acl_bind_opts.group != "":
        group_acl_template = kafka_admin.AclBinding(
            restype=kafka_admin.ResourceType.GROUP, 
            name=acl_bind_opts.group, 
            resource_pattern_type=resource_pattern_type,
            principal=None if acl_bind_opts.principal == "" else acl_bind_opts.principal,
            host="*",
            operation=kafka_admin.AclOperation.ANY,
            permission_type=kafka_admin.AclPermissionType.ALLOW
        )
        assign_operation_acl(acl_binding_filter_list=acl_binding_list,
                             acl_filter_template=group_acl_template,
                             producer=acl_bind_opts.producer,
                             consumer=acl_bind_opts.consumer, 
                             operations=acl_bind_opts.operations)
    remove_acl_duplicates(acl_binding_list)
    if len(acl_binding_list) == 0:
        print("[red] acl_binding_list is empty [/red]")
        raise typer.Abort()

    return acl_binding_list

def create_acl_binding_filter_list(acl_bind_opts: acl_bindings.acl_binding_options) -> list[kafka_admin.AclBindingFilter]:
    """
    Creates a list of AclBindingFilter objects based on the provided ACL binding options.

    Args:
        acl_bind_opts (acl_bindings.acl_binding_options): The ACL binding options.

    Returns:
        list[kafka_admin.AclBindingFilter]: A list of AclBindingFilter objects.

    Raises:
        typer.Abort: If specific ACL operations are provided along with the producer or consumer flag, or if the 
        resulting acl_binding_filter_list is empty.
    """
    if acl_bind_opts.all:
        default_acl_binding_filter = kafka_admin.AclBindingFilter(
            restype=kafka_admin.ResourceType.ANY,
            name=None,
            resource_pattern_type=kafka_admin.ResourcePatternType.ANY,
            principal=None,
            host=None,
            operation=kafka_admin.AclOperation.ANY,
            permission_type=kafka_admin.AclPermissionType.ANY
        )
        return [default_acl_binding_filter]

    if ('*' in acl_bind_opts.principal) or (not acl_bind_opts.prefixed and ('*' in acl_bind_opts.topic or '*' in acl_bind_opts.group)):
        print("[yellow] wildcard found in topic/group/principal, will fetch current acl list and filter matching acls with LITERAL pattern[/yellow]")
        print("[red] if you are searching for PREFIXED resource pattern type, use --prefixed instead[/red]")
        filtered_acl_binding_list = filter_existing_acls(acl_bind_opts)
        return filtered_acl_binding_list

    if len(acl_bind_opts.operations) > 0 and (acl_bind_opts.producer or acl_bind_opts.consumer):
        print("[red]filter with specific acl operations doesn't work with consumer or producer flag[/red]")
        raise typer.Abort()

    acl_binding_filter_list = []
    if acl_bind_opts.prefixed:
        resource_pattern_type = kafka_admin.ResourcePatternType.PREFIXED
    else:
        resource_pattern_type = kafka_admin.ResourcePatternType.LITERAL

    if acl_bind_opts.topic:
        topic_acl_filter_template = kafka_admin.AclBindingFilter(
            restype=kafka_admin.ResourceType.TOPIC, 
            name=acl_bind_opts.topic, 
            resource_pattern_type=resource_pattern_type,
            principal=None if acl_bind_opts.principal == "" else acl_bind_opts.principal,
            host="*",
            operation=kafka_admin.AclOperation.ANY,
            permission_type=kafka_admin.AclPermissionType.ALLOW
        )
        assign_operation_acl(acl_binding_filter_list=acl_binding_filter_list,
                             acl_filter_template=topic_acl_filter_template,
                             producer=acl_bind_opts.producer,
                             consumer=acl_bind_opts.consumer, 
                             operations=acl_bind_opts.operations)

    if acl_bind_opts.group:
        group_acl_filter_template = kafka_admin.AclBindingFilter(
            restype=kafka_admin.ResourceType.GROUP, 
            name=acl_bind_opts.group, 
            resource_pattern_type=resource_pattern_type,
            principal=None if acl_bind_opts.principal == "" else acl_bind_opts.principal,
            host="*",
            operation=kafka_admin.AclOperation.ANY,
            permission_type=kafka_admin.AclPermissionType.ALLOW
        )
        assign_operation_acl(acl_binding_filter_list=acl_binding_filter_list,
                             acl_filter_template=group_acl_filter_template,
                             producer=acl_bind_opts.producer,
                             consumer=acl_bind_opts.consumer, 
                             operations=acl_bind_opts.operations)
    remove_acl_duplicates(acl_binding_filter_list)
    if len(acl_binding_filter_list) == 0:
        print("[red] acl_binding_filter_list is empty [/red]")
        raise typer.Abort()

    return acl_binding_filter_list

def filter_existing_acls(acl_bind_opts: acl_bindings.acl_binding_options) -> list[kafka_admin.AclBindingFilter]:
    acls =  get_all_acl_bindings(acl_bind_opts.admin)
    if acls is None or len(acls) == 0:
        print("[red] no acls found, skipping filter[/red]")
        return []

    def acl_get_fun_topic(acl: kafka_admin.AclBinding):
        if acl.restype == kafka_admin.ResourceType.TOPIC and acl.resource_pattern_type == kafka_admin.ResourcePatternType.LITERAL:
            return acl.name
        else:
            return ""
    def acl_get_fun_group(acl: kafka_admin.AclBinding):
        if acl.restype == kafka_admin.ResourceType.GROUP and acl.resource_pattern_type == kafka_admin.ResourcePatternType.LITERAL:
            return acl.name
        else:
            return ""
    def acl_get_fun_principal(acl: kafka_admin.AclBinding):
        return acl.principal
    acl_binding_filter_list = []
    if (not acl_bind_opts.prefixed) and ('*' in acl_bind_opts.topic):
        for acl in acls:
            if match_acl_query(acl_get_fun_topic, acl_bind_opts.topic, acl):
                acl_binding_filter_list.append(acl_binding_to_filter(acl))

    if (not acl_bind_opts.prefixed) and ('*' in acl_bind_opts.group):
        for acl in acls:
            if match_acl_query(acl_get_fun_group, acl_bind_opts.group, acl):
                acl_binding_filter_list.append(acl_binding_to_filter(acl))

    if '*' in acl_bind_opts.principal:
        for acl in acls:
            if match_acl_query(acl_get_fun_principal, acl_bind_opts.group, acl):
                acl_binding_filter_list.append(acl_binding_to_filter(acl))
    
    return acl_binding_filter_list

def match_acl_query(acl_get_fun: callable, acl_regex_query: str, acl: kafka_admin.AclBinding):
    acl_type_query: str = acl_get_fun(acl)
    if re.match(acl_regex_query, acl_type_query):
        return True
    else:
        return False
    

def remove_acl_duplicates(acl_binding_filter_list: list[kafka_admin.AclBindingFilter]):
    """
    Removes duplicate ACL bindings from the provided list.

    Args:
        acl_binding_filter_list (list[kafka_admin.AclBindingFilter]): The list of ACL bindings.
    """
    existing_key = []
    unique_acl_binding_filters = []
    for acl_binding_filter in acl_binding_filter_list:
        key = create_key(acl_binding_filter)
        if key not in existing_key:
            unique_acl_binding_filters.append(acl_binding_filter)
            existing_key.append(key)
    acl_binding_filter_list[:] = unique_acl_binding_filters
        # else:
        #     existing_key.append(key)

def create_key(acl_binding_filter: kafka_admin.AclBindingFilter):
    """
    Creates a unique key for the given ACL binding filter.

    Args:
        acl_binding_filter (kafka_admin.AclBindingFilter): The ACL binding filter.

    Returns:
        str: The unique key for the ACL binding filter.
    """
    return f"{acl_binding_filter.name}:{acl_binding_filter.operation_int}:{acl_binding_filter.principal}:{acl_binding_filter.restype_int}:{acl_binding_filter.permission_type_int}:{acl_binding_filter.host}"

def assign_new_operation(acl_binding_filter: kafka_admin.AclBindingFilter, operation: kafka_admin.AclOperation):
    """
    Assigns a new operation to the given ACL binding filter.

    Args:
        acl_binding_filter (kafka_admin.AclBindingFilter): The ACL binding filter.
        operation (kafka_admin.AclOperation): The new operation to assign.
    """
    acl_binding_filter.operation = operation
    acl_binding_filter.operation_int = operation.value

def assign_operation_acl(acl_binding_filter_list: list[kafka_admin.AclBindingFilter], acl_filter_template: kafka_admin.AclBindingFilter, producer: bool = False, consumer: bool = False, operations: list[kafka_admin.AclOperation] = []):
    """
    Assigns operations to the ACL binding filter list based on the provided options.

    Args:
        acl_binding_filter_list (list[kafka_admin.AclBindingFilter]): The list of ACL binding filters.
        acl_filter_template (kafka_admin.AclBindingFilter): The ACL filter template.
        producer (bool, optional): Whether to assign producer operations. Defaults to False.
        consumer (bool, optional): Whether to assign consumer operations. Defaults to False.
        operations (list[kafka_admin.AclOperation], optional): The list of specific operations to assign. Defaults to [].
    """
    if len(operations) > 0:
        for operation in operations:
            acl_filter = copy.deepcopy(acl_filter_template)
            assign_new_operation(acl_filter, operation)
            acl_binding_filter_list.append(acl_filter)
    if consumer:
        acl_binding_filter_list.extend(create_consumer_acl_filter(acl_filter_template))
    if producer:
        acl_binding_filter_list.extend(create_producer_acl_filter(acl_filter_template))

    if len(acl_binding_filter_list) > 0:
        return
    
    if not (consumer and producer):
        acl_binding_filter_list.append(acl_filter_template)
        return
    
    return

def create_consumer_acl_filter(acl_filter: kafka_admin.AclBindingFilter):
    """
    Creates a list of consumer ACL filters based on the provided ACL filter template.

    Args:
        acl_filter (kafka_admin.AclBindingFilter): The ACL filter template.

    Returns:
        list[kafka_admin.AclBindingFilter]: A list of consumer ACL filters.
    """
    consumer_filter = [copy.deepcopy(acl_filter), copy.deepcopy(acl_filter)]
    assign_new_operation(consumer_filter[0], kafka_admin.AclOperation.READ)
    assign_new_operation(consumer_filter[1], kafka_admin.AclOperation.DESCRIBE)
    return consumer_filter

def create_producer_acl_filter(acl_filter: kafka_admin.AclBindingFilter):
    """
    Creates a list of producer ACL filters based on the provided ACL filter template.

    Args:
        acl_filter (kafka_admin.AclBindingFilter): The ACL filter template.

    Returns:
        list[kafka_admin.AclBindingFilter]: A list of producer ACL filters.
    """
    producer_filter = [copy.deepcopy(acl_filter), copy.deepcopy(acl_filter), copy.deepcopy(acl_filter)]
    assign_new_operation(producer_filter[0], kafka_admin.AclOperation.WRITE) 
    assign_new_operation(producer_filter[1], kafka_admin.AclOperation.DESCRIBE) 
    assign_new_operation(producer_filter[2], kafka_admin.AclOperation.CREATE) 
    return producer_filter

def get_acl(admin: kafka_admin.AdminClient, acl_binding_filter: kafka_admin.AclBindingFilter):
    """
    Retrieves ACLs from the Kafka admin client based on the provided ACL binding filter.

    Args:
        admin (kafka_admin.AdminClient): The Kafka admin client.
        acl_binding_filter (kafka_admin.AclBindingFilter): The ACL binding filter.

    Returns:
        list: A list of ACL bindings.
    """
    try:
        describe_future = admin.describe_acls(acl_binding_filter, request_timeout=10)
    except Exception as e:
        print(f"get_acl() failed: {e}")
        return

    try:
        while describe_future.running():
            if describe_future.cancelled:
                pass
            continue
        acls_result = describe_future.result()
        return acls_result
    except confluent_kafka.error.KafkaException as e:
        print("Failed to get {}".format(e))
    except Exception:
        raise    
    
def delete_acl(admin: kafka_admin.AdminClient, acl_binding_filters: kafka_admin.AclBindingFilter):
    """
    Deletes ACLs from the Kafka admin client based on the provided ACL binding filters.

    Args:
        admin (kafka_admin.AdminClient): The Kafka admin client.
        acl_binding_filters (kafka_admin.AclBindingFilter): The ACL binding filters.
    """
    try:
        delete_futures = admin.delete_acls(acl_binding_filters, request_timeout=10)
    except Exception as e:
        print(f"[red]Error deleting ACLs: {e}[/red]")
        return

    for delete_result, f in delete_futures.items():
        try:
            acl_bindings = f.result()
            print(f"Deleted acls matching filter: {delete_result}")
            if len(acl_bindings) == 0:
                print("[red] nothing in acl_bindings found:[/red] skipping \n")
            for acl_binding in acl_bindings:
                print(f"deleting acl_binding: [green] {acl_binding} [/green]  \n")

        except confluent_kafka.error.KafkaException as e:
            print(f"Failed to delete {delete_result}: {e} \n")
        except Exception:
            raise

def create_acl(admin: kafka_admin.AdminClient, acl_binding: kafka_admin.AclBinding):
    """
    Creates ACLs in the Kafka admin client based on the provided ACL binding.

    Args:
        admin (kafka_admin.AdminClient): The Kafka admin client.
        acl_binding (kafka_admin.AclBinding): The ACL binding.
    """
    try:
        create_futures = admin.create_acls(acl_binding, request_timeout=30)
    except Exception as e:
        print(f"create_acl() failed: {e}")
        return

    for create_result, f in create_futures.items():
        try:
            future_result = f.result()
            if future_result is None:
                print("Created acls matching filter: [green] {} [/green]".format(create_result))
            else:
                print(f"error in create future:[red] {future_result}[/red]")
        except confluent_kafka.error.KafkaException as e:
            print("Failed to create {}: {}".format(create_result, e))
        except Exception:
            raise

def kafka_operations_list(kafka_operations_string: str) -> list[kafka_admin.AclOperation]:
    """
    Converts a comma-separated string of Kafka operations to a list of AclOperation objects.

    Args:
        kafka_operations_string (str): The comma-separated string of Kafka operations.

    Returns:
        list[kafka_admin.AclOperation]: A list of AclOperation objects.
    """
    kafka_operations_list = []
    operations_string = kafka_operations_string.split(',')
    for operation in operations_string:
        ops = kafka_operations(operation)
        if ops != "":
            kafka_operations_list.append(kafka_operations(operation))
    return kafka_operations_list

def kafka_operations(string_operations: str) -> kafka_admin.AclOperation:
    """
    Converts a string representation of a Kafka operation to an AclOperation object.

    Args:
        string_operations (str): The string representation of the Kafka operation.

    Returns:
        kafka_admin.AclOperation: The corresponding AclOperation object.
    """
    match string_operations:
        case "write":
            return kafka_admin.AclOperation.WRITE
        case "read":
            return kafka_admin.AclOperation.READ
        case "create":
            return kafka_admin.AclOperation.CREATE
        case "delete":
            return kafka_admin.AclOperation.DELETE
        case "describe":
            return kafka_admin.AclOperation.DESCRIBE
        case "all":
            return kafka_admin.AclOperation.ALL
        case "":
            return ""
        case _:
            print(f"[red] operations {string_operations} not found")
            return ""

def print_acl(acl: kafka_admin.AclBinding):
    """
    Prints the provided ACL bindings.

    Args:
        acls (list[kafka_admin.AclBinding]): The ACL bindings.
    """     
 
    acl_result_string_format = f"""
    [yellow]name[/yellow] = [green]{acl.name}[/green]
    [yellow]host[/yellow] = [green]{acl.host}[/green]
    [yellow]operation[/yellow] = [green]{acl.operation}[/green]
    [yellow]principal[/yellow] = [green]{acl.principal}[/green]
    [yellow]resource_type[/yellow] = [green]{acl.restype}[/green]
    [yellow]resource_pattern_type[/yellow] = [green]{acl.resource_pattern_type}[/green]
    [yellow]permission_type[/yellow] = [green]{acl.permission_type}[/green]"""
    
    print(acl_result_string_format)

def acl_binding_to_filter(acl_binding: kafka_admin.AclBinding) -> kafka_admin.AclBindingFilter:
    """
    Converts an AclBinding object to an AclBindingFilter object.

    Args:
        acl_binding (kafka_admin.AclBinding): The AclBinding object.

    Returns:
        kafka_admin.AclBindingFilter: The corresponding AclBindingFilter object.
    """
    return kafka_admin.AclBindingFilter(
        restype=acl_binding.restype,
        name=acl_binding.name,
        resource_pattern_type=acl_binding.resource_pattern_type,
        principal=acl_binding.principal,
        host=acl_binding.host,
        operation=acl_binding.operation,
        permission_type=acl_binding.permission_type
    )
