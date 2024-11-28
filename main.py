from swarm import Swarm, Agent

# 利用OpenAI的接口（安装swarm时会自动下载），建立与ollama服务连接的客户端
from openai import OpenAI

from tabulate import tabulate

from flask import Flask, request, jsonify
from flask_cors import CORS

import requests
import re

ollama_client = OpenAI(
    base_url='https://dashscope.aliyuncs.com/compatible-mode/v1',
    api_key='test',
)

# 在swarm构建时，指定与ollama连接的客户端
client = Swarm(client=ollama_client)


# 全局上下文
messages = []


def clear_message():
    global messages
    messages = []


def instructions_dispatch(context_variables):
    return """
    任务指令：根据用户提供的中文自然语言，提取要执行的操作，并调用对应的agent执行。 如识别到的操作不在agent列表中，或无法识别操作，则提示用户很抱歉，无法执行此操作，
    支持的操作如下： 
      - 退返单查询
      - 页面访问查询
      - sellout查询
      - 用户登录记录查询"""


# 执行上下文prompt
def instructions_page_access(context_variables):
    return """
    任务指令：根据用户提供的中文自然语言生成SQL查询语句，并按照指定步骤执行。请注意，你只能查询页面访问数据，其他查询需调用别的agent。 用户如果没有给查询条件，则提示用户补充。一次只能执行1个SQL.
    步骤如下： 1. 根据用户输入的中文自然语言生成一个SQL查询语句。 2. 调用`clean_sql`函数对生成的SQL进行清理。 3. 调用`execute_sql`函数执行清理后的SQL，返回查询结果。 
    生成SQL查询语句的要求： 
        - 只返回SQL查询，不要包含任何其他文本或解释。 
        - 支持复杂查询，包括多表连接、比较、排序、JSON筛选和聚合函数。 
        - 起别名的时候不要和字段名重复
        - 不要使用DuckDB v1.1版本的函数。 
        - 函数替换： 
          - `CURRENT_TIMESTAMP` 替换为 `current_date` 
          - `TO_CHAT` 替换为 `CAST` 
          - `TOP` 替换为 `LIMIT`
        数据库信息： 
        - 数据库类型：DuckDB 
        - 数据库版本：V1.0.0 
        - 表名：event_data 
        - 字段信息： 
          - `website_id`: VARCHAR 
          - `data_key`: VARCHAR (`PAGE_DURATION`表示页面访问数据) 
          - `string_value`: VARCHAR (是否JSON: 是)，其内部结构如下： 
            - `duration`: DOUBLE 
            - `userName`: VARCHAR 
            - `userId`: VARCHAR 
            - `userType`: INTEGER (1表示内部用户, 0表示外部用户) 
            - `level1`: VARCHAR 
          - `created_at`: TIMESTAMP 特殊说明： 
    - 只查询`website_id`等于`9841d974-79c7-428b-a871-da95c1713bb1`的数据。 
    - 请注意字段string_value是json结构，如需查询或筛选json内字段，请用duckdbV1.0版本函数先提取出来
    - 确保生成的SQL符合上述要求，并且能够正确执行。 
    - 在生成SQL时，请考虑所有给定的字段信息和特殊字段处理。"""


def instructions_sellout(context_variables):
    return """
    任务指令：根据用户提供的中文自然语言生成SQL查询语句，并按照指定步骤执行。请注意，你只能查询sellout数据，其他查询需调用别的agent。 用户如果没有给查询条件，则提示用户补充。金额不要使用科学计数法展示.一次只能执行1个SQL.
    步骤如下： 1. 根据用户输入的中文自然语言生成一个SQL查询语句。 2. 调用`clean_sql`函数对生成的SQL进行清理。 3. 调用`execute_sql_sellout`函数执行清理后的SQL，返回查询结果。 
    生成SQL查询语句的要求： 
        - 只返回SQL查询，不要包含任何其他文本或解释。 
        - 支持复杂查询，包括多表连接、比较、排序、JSON筛选和聚合函数。 
        - 起别名的时候不要和字段名重复
        - 不要使用DuckDB v1.1版本的函数。 
        - 函数替换： 
          - `CURRENT_TIMESTAMP` 替换为 `current_date` 
          - `TO_CHAT` 替换为 `CAST` 
          - `TOP` 替换为 `LIMIT`
        数据库信息： 
        - 数据库类型：DuckDB 
        - 数据库版本：V1.0.0 
        - 表名：T_SO_STOCK_COLLECT_DATA 
        - 字段信息： 
          - ID: INTEGER
          - MAIN_ID: INTEGER, 上报ID
          - SOURCE: VARCHAR(来源 EDI 表示 EDI上报, WEB 表示 网页上报) 
          - YEAR: INTEGER, 年
          - MONTH: INTEGER, 月
          - TEMPLATE_ID: INTEGER, 模板ID
          - TEMPLATE_NAME: VARCHAR, 模板名称
          - CUSTOMER_CODE: VARCHAR, 客户编号
          - CUSTOMER_NAME: VARCHAR, 客户名称
          - PARENT_COMPANY_CODE: VARCHAR, 实际母公司代码
          - CUSTOMER_PROVINCE_CODE: VARCHAR, 省份编码
          - SUB_CUSTOMER_CODE: VARCHAR, 下游客户编码
          - SUB_CUSTOMER_NAME: VARCHAR, 下游客户名称
          - BU_CODE: VARCHAR, 产品所属BU
          - PRODUCT_GROUP_CODE: VARCHAR, 产品组编码
          - PRODUCT_GROUP_NAME: VARCHAR, 产品组名称
          - PRODUCT_LINE_CODE: VARCHAR, 产品线编号
          - PRODUCT_LINE_NAME: VARCHAR, 产品线名称
          - MPG: VARCHAR, MPG
          - MATERIAL_CODE: VARCHAR, 物料号
          - PRODUCT_SPECIFICATION: VARCHAR, 产品规格
          - ACTUAL_AMOUNT: DOUBLE, 月实际销售额（折扣价）
          - PROJECT_ACTUAL_AMOUNT: DOUBLE, 项目出货 月实际销售额（折扣价）
          - RETAIL_ACTUAL_AMOUNT: DOUBLE, 零售出货 月实际销售额（折扣价）
          - HOME_ACTUAL_AMOUNT: DOUBLE0, 家装出货月实际销售额（折扣价）
          - ONLINE_ACTUAL_AMOUNT: DOUBLE, 电商出货月实际销售额（折扣价）
          - UNIT_PRICE: DOUBLE, 单价
          - QUANTITY: INTEGER, 库存数量
          - CURRENT_AMOUNT: DOUBLE, 目前库存金额（折扣价）
          - BEGIN_QUANTITY: INTEGER, 月初库存量（只）
          - IN_QUANTITY: INTEGER, 当月到货量（只）
          - OUT_QUANTITY: INTEGER, 当月出货量（只）
          - AMOUNT: DOUBLE, 库存金额（元）
          - DELETE_FLAG: VARCHAR(删除标记 N表示未删除, Y表示已删除)
          - CREATE_TIME: TIMESTAMP, 创建时间
          - UPDATE_TIME: TIMESTAMP, 修改时间
          - CREATE_BY: INTEGER, 创建人
          - UPDATE_BY: INTEGER, 修改人
          - UPDATE_BY_NAME: VARCHAR, 修改人名称
          - ENABLE: INTEGER(是否有效 1表示有效, 0表示无效)
          - CUSTOMER_PROVINCE_NAME: VARCHAR, 省份名称
          - RESPONSIBLE_PERSON: VARCHAR, 客户负责人
          - RESPONSIBLE_PERSON_BU: VARCHAR, 客户负责人BU
    - 确保生成的SQL符合上述要求，并且能够正确执行。 
    - 在生成SQL时，请考虑所有给定的字段信息和特殊字段处理。"""


def instructions_return_order(context_variables):
    return """
    任务指令：从用户提供的中文自然语言提取关键信息，并调用function进行查询退返单。请注意，你只能查询退返单数据。其他查询需调用别的agent。 用户如果没有给查询条件，则提示用户补充。
    关键信息如下： 
      - 退返类型（必填，包括{name:好货退货,key:GOOD_GOODS_RETURN}和{name:年度退货,key:ANNUAL_RETURN}）
      - 退返单号（必填，格式为以字段R开头 + 4位年份 + 2位月份 + 6位数字编码）
      - 客户代码（选填，格式为6位数字）
    关键信息有key的话，用key调用接口。
    所有关键信息都需要从用户输入的信息中获取，不要自己编造。关键信息如有指定格式的，需要严格按照格式匹配。关键信息如有指定内容的，需能匹配指定内容
    关键信息以外的内容不要让用户提供
    必填的关键信息如用户没有提供，或提供的格式不正确，需要提示用户补充未提供的关键信息
    选填的关键信息用户没有提供的情况下，不需要提示用户输入，请求接口的时候传递空即可"""


def instructions_login_record(context_variables):
    return """
    任务指令：根据用户提供的中文自然语言生成SQL查询语句，并按照指定步骤执行。请注意，你只能查询用户登录数据，其他查询需调用别的agent。 用户如果没有给查询条件，则提示用户补充。一次只能执行1个SQL.
    步骤如下： 1. 根据用户输入的中文自然语言生成一个SQL查询语句。 2. 调用`clean_sql`函数对生成的SQL进行清理。 3. 调用`execute_sql_login`函数执行清理后的SQL，返回查询结果。 
    生成SQL查询语句的要求： 
        - 只返回SQL查询，不要包含任何其他文本或解释。 
        - 支持复杂查询，包括多表连接、比较、排序和聚合函数。 
        - 起别名的时候不要和字段名重复
        数据库信息： 
        - 数据库类型：Oracle
        - 表名：T_USER_LOGIN_RECORD 
        - 字段信息： 
          - `USER_ID`: NUMBER (登录用户ID 关联T_USER表ID)
          - `SESA`: VARCHAR (登录用户SESA) 
          - `CLIENT`: VARCHAR (登录源 PC代表'电脑端' PHONE代表'小程序')
          - `CREATE_TIME`: DATE (登录时间)
        - 表名：T_USER 
        - 字段信息： 
          - `ID`: NUMBER (登录用户ID)
          - `CUSTOMER_CODE`: VARCHAR (绑定客户代码) 
          - `FULL_NAME`: VARCHAR (全名)
          - `USER_TYPE`: VARCHAR (用户分类0代表'客户用户' 1代表'施耐德用户')： 
          - `CREATE_TIME`: DATE (用户创建时间)
    - 确保生成的SQL符合上述要求，并且能够正确执行。 
    - 在生成SQL时，请考虑所有给定的字段信息和特殊字段处理。
    - 如SQL执行后没有返回结果，直接告诉用户暂未查询到数据即可"""


def clean_sql(sql_query):
    print(f"SQL清理 {sql_query}")
    """清理SQL查询，移除可能的Markdown格式和多余空白"""
    cleaned = re.sub(r'```sql\s*|\s*```', '', sql_query).strip().replace('\n', ' ').replace('\r', ' ').replace(';', ' ')
    return cleaned


# SQL执行
def execute_sql(sql_query):
    """执行SQL查询并返回结果"""
    response = requests.post('http://10.155.101.199:8450/data-website/database/execute',
                             json={"database": "website", "params": [], "sql": sql_query})
    clear_message()
    if response.status_code == 200:
        data = response.json()
        print(f"查询结果 {data}")
        return format_results(data.get('body', {}))
    else:
        print(f"Request failed with status code {response.status_code}")
    return ""


def execute_sql_sellout(sql_query):
    """执行SQL查询并返回结果"""
    response = requests.post('http://10.155.101.199:9040/data-sellout/database/query',
                             json={"database": "sellout", "params": [], "sql": sql_query})
    clear_message()
    if response.status_code == 200:
        data = response.json()
        print(f"查询结果 {data}")
        return format_results(data.get('body', {}))
    else:
        print(f"Request failed with status code {response.status_code}")
    return ""


def search_return_order(return_order_type, return_order_no, customer_code):
    user_id = 1
    entity_code = 'CN01'
    user_type = 1
    base_url = 'http://internal-cnn-lb-rg-awsk8s-dsp-1848782492.cn-north-1.elb.amazonaws.com.cn/api'
    api_url = f'/returnrepairs/business?sessionUserId={user_id}&sessionEntityCode={entity_code}&userType={user_type}'
    param = f'&busRrType={return_order_type}&busRrNo={return_order_no}&customerCode={customer_code}&offset=1&limit=2'
    """执行SQL查询并返回结果"""
    response = requests.get(base_url+api_url+param)
    clear_message()
    if response.status_code == 200:
        data = response.json()
        print(f"查询结果 {data}")
        return data.get('body', {})
    else:
        print(f"Request failed with status code {response.status_code}")

    return ""


def execute_sql_login(sql_query):
    """执行SQL查询并返回结果"""
    base_url = 'http://internal-cnn-lb-rg-awsk8s-dsp-1848782492.cn-north-1.elb.amazonaws.com.cn/api'
    api_url = f'/master-data/event-tracking/loginRecord/statistic/sql?'
    param = f'sql={sql_query}'
    response = requests.get(base_url+api_url+param)
    clear_message()
    if response.status_code == 200:
        data = response.json()
        print(f"查询结果 {data}")
        return data.get('body', {})
    else:
        print(f"Request failed with status code {response.status_code}")
    return ""


# 结果格式化
def format_results(results):
    print(f"结果格式化 {results}")
    results_rows = results.get('rows', {})
    results_columns = results.get('columns', {})
    """格式化查询结果，添加上下文和单位"""
    if not results or len(results_rows) == 0:
        return "没有找到匹配的结果。"

    results_rows.insert(0, results_columns)

    formatted_results = tabulate(results_rows, headers='firstrow', tablefmt="fancy_grid")

    return formatted_results


# agent 定义
agent_dispatch = Agent(
    name="Agent_Dispatch",
    model="qwen2.5-72b-instruct",
    instructions=instructions_dispatch,
    tool_choice="auto"
)


agent_sql_page_access = Agent(
    name="SQLAgent_page_access",
    # 在构建智能体时指定ollama中的模型，传入在ollama中构建好的大模型名称即可，例如qwen2.5:7b
    model="qwen2.5-72b-instruct",
    # model="qwen2.5-coder",
    # model="llama3.2",
    # model="codellama",
    instructions=instructions_page_access,
    tool_choice="auto",
    functions=[clean_sql, execute_sql]
)


agent_sql_sellout = Agent(
    name="SQLAgent_sellout",
    model="qwen2.5-72b-instruct",
    instructions=instructions_sellout,
    tool_choice="auto",
    functions=[clean_sql, execute_sql_sellout]
)


agent_search_return_order = Agent(
    name="SearchAgent_return_order",
    model="qwen2.5-72b-instruct",
    instructions=instructions_return_order,
    tool_choice="auto",
    functions=[search_return_order]
)


agent_search_login_record = Agent(
    name="SearchAgent_login_record",
    model="qwen2.5-72b-instruct",
    instructions=instructions_login_record,
    tool_choice="auto",
    functions=[clean_sql, execute_sql_login]
)


# functions
def transfer_to_agent_sql_sellout():
    return agent_sql_sellout


def transfer_to_agent_sql_page_access():
    return agent_sql_page_access


def transfer_to_agent_search_return_order():
    return agent_search_return_order


def transfer_to_agent_search_login_record():
    return agent_search_login_record


# 切换function
agent_dispatch.functions.append(transfer_to_agent_sql_page_access)
agent_dispatch.functions.append(transfer_to_agent_sql_sellout)
agent_dispatch.functions.append(transfer_to_agent_search_return_order)
agent_dispatch.functions.append(transfer_to_agent_search_login_record)


# 调用过程
def process_query(natural_language_query, messages):
    """处理自然语言查询，转换为SQL，执行并返回结果"""
    # 使用 Swarm 将自然语言转换为 SQL
    # print(f"上下文：{messages}")
    messages.append({"role": "user", "content": natural_language_query})
    response = client.run(
        messages=messages,
        agent=agent_dispatch,
    )
    result = response.messages[-1]["content"]
    # messages.append({"role": "assistant", "content": result})

    print(f"AI整理后结果:\n{result}")
    return result


app = Flask(__name__)
CORS(app)


@app.route('/api/chat', methods=['POST'])
def chat():
    data = request.json
    user_message = data.get('message', '')
    response = process_query(user_message, messages)

    return jsonify({'response':response})

# 主程序循环
if __name__ == "__main__":
    app.run(host='127.0.0.1',port=5000)
    # print("欢迎使用Mycp智能查询助手！")
    # print("输入 'exit' 或 'quit' 退出程序。")
    # print("本系统支持商务退返单查询，埋点数据查询和sellout数据查询。")
    #
    # while True:
    #     user_input = input("\n请输入您的查询 (或 'exit' 退出): ")
    #     if user_input.lower() in ['exit', 'quit']:
    #         print("谢谢使用，再见！")
    #         break
    #
    #     result = process_query(user_input, messages)
    #     print(result)
        # messages.append({"role": "assistant", "content": result})
