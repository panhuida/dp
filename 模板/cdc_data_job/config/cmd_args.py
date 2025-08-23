from optparse import OptionParser

import config.default_args as default_args


def create_parser():
    """
    create command line parser
    """
    # 规定用法
    usage = "-s server_addresses -n namespace -u username -p password"
    # 构造实例对象
    parser = OptionParser(add_help_option=True, description="", usage=usage)
    # 短名-s，长名--server_addresses，对应server_addresses
    parser.add_option('-s', '--server_addresses', dest='server_addresses')
    # 短名-n，长名--namespace，对应namespace
    parser.add_option('-n', '--namespace', dest='namespace')
    # 短名-u，长名--username，对应username
    parser.add_option('-u', '--username', dest='username')
    # 短名-p，长名--password，对应password
    parser.add_option('-p', '--password', dest='password')
    # 短名-d，长名--data_id，对应data_id
    parser.add_option('-d', '--data_id', dest='data_id')
    # 短名-g，长名--group，对应group
    parser.add_option('-g', '--group', dest='group')
    # 短名-f，长名--fle，对应file
    parser.add_option('-f', '--file', dest='sql_file')
    # 长名--sql_list_file，对应sql_list_file
    parser.add_option('--sql_list_file', dest='sql_list_file')
    # 短名-sp 长名--sql_params, 对应sql参数值
    parser.add_option('--sql_params', dest='sql_params')

    return parser


def get_nacos_params(parser):
    # 获取传参，options为关键字传参，args其他参数
    (options, args) = parser.parse_args()
    # 若有此关键字传参，则赋值
    if options.server_addresses:
        server_addresses = options.server_addresses
    else:
        server_addresses = default_args.server_addresses
    if options.namespace:
        namespace = options.namespace
    else:
        namespace = default_args.namespace
    if options.username:
        username = options.username
    else:
        username = default_args.username
    if options.password:
        password = options.password
    else:
        password = default_args.password
    if options.data_id:
        data_id = options.data_id
    else:
        data_id = default_args.data_id
    if options.group:
        group = options.group
    else:
        group = default_args.group

    return {'server_addresses': server_addresses, 'namespace': namespace, 'username': username,
            'password': password, 'data_id': data_id, 'group': group}


def get_sql_params(parser):
    # 获取传参，options为关键字传参，args其他参数
    (options, args) = parser.parse_args()
    if options.sql_params:
        sql_params = options.sql_params
    else:
        sql_params = None
    if options.sql_file:
        sql_file = options.sql_file
        mapping_name = sql_file.split('/')[-1].split('.')[0].lower()
    else:
        sql_file = None
        mapping_name = None
    if options.sql_list_file:
        sql_list_file = options.sql_list_file
    else:
        sql_list_file = None

    param_items = {'sql_file': sql_file, 'sql_list_file': sql_list_file, 'mapping_name': mapping_name,
                   'parameter_values': sql_params}
    if sql_params is not None:
        if ',' in sql_params:
            param_str_items = sql_params.split(',')
        else:
            param_str_items = [sql_params]
    else:
        param_str_items = None

    if param_str_items is not None and len(param_str_items) > 0:
        for item in param_str_items:
            item_map = item.split('=')
            param_items[item_map[0]] = item_map[1]

    return param_items


if __name__ == '__main__':
    print(default_args.group)
    test_parser = create_parser()
    nacos_params = get_nacos_params(test_parser)
    sql_params1 = get_sql_params(test_parser)
    print(nacos_params)
    print(sql_params1)
