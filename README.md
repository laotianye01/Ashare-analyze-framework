# 0. 闲言碎语
我本人本科为计算机类的专业，研究生则学习了金融科技有关的内容。我为了重温本科所学的软工，网页，python，数据库，ai等内容，外加希望写个帮助自身随时获取股票信息并筛选的程序，就心血来潮写了这么个程序，以便加深自己对这两方面知识的理解。这个程序使用了akshare提供的金融数据接口，搭建了一个可以自动化分析大盘数据分析的框架（虽然有现成的airflow，但是想自己试试看自己能不能写个玩玩），你可以使用json配置任务流程，在里面调用task中定义的节点，并使用UI去执行。（其是实现对应的功能不用这么复杂，直接手写函数调用akshare+周期性执行对应分析与ai api调用部分代码就可以，调度可以使airflow，但是我就是想着既然学了这些知识，那为什么不写一个大一点的程序试试呢，于是就有了这些代码）

* 运行示例
https://github.com/user-attachments/assets/978976ef-db0b-4fd9-aaa9-573f97ce77e7

**使用项目**

* akshare项目提供了大量的a股数据api，易于使用
  参考文档: https://akshare.akfamily.xyz/tutorial.html

# 1. 项目主体结构

```
# 核心项目调度代码
core/  
  ├── resource/                        # 此处定义了执行任务所需要实例化的资源
      ├── __init__.py                  # 构建了资源的工厂实例化方法
      ├── agent.py                     # 定义了LLM模型类
      ├── LLMDatabase.py               # 定义了长期向量数据库管理类              
      ├── postgre.py                   # 定义了数据库接口类
      ├── database_table.py            # 使用了ORM范式定义了数据库中表的架构
      └── searcher.py                  # 定义了搜索引擎
  ├── logger.py                        # 日志记录
  ├── pipeline.py                      # 执行的任务流程类，定义了任物流信息，存储了对应的task实例
  ├── taskNode.py                      # 每一个具体的任务
  ├── resource_manager.py              # 资源管理器
  └── scheduler.py                     # 任务调度器（通过task，pipeline，pipelineExecuter与scheduler管理）

# 有关工具与库
utils/                                 
  ├── database_utils.py                # 包含数据库相关操作
  ├── dataframe_utils.py               # 包含了获取到的dataframe的相关处理函数
  ├── embedding_module.py              # 用于LLM向量数据库中的向量化操作
  ├── MyTT.py                          # 传统金融分析工具
  ├── stock_database.py                # ORM 下postgre数据库中表的定义，用于数据库的设计
  ├── load_json.py                     # 用于处理配置json
  └── html_parser.py                   # 用于处理爬虫获取的html界面

# postgre数据库检查与迭代工具（由alembic自动生成）
postgre_check_tool/
  └── ... 

# 数据临时存储与函数测试中间输出存储
cache/                                 
  ├──
  └── ... 

# 函数测试代码
function_test/                                 
  ├──
  └── ... 

# akshare包，提供了有关股票数据获取的函数
akshare/                                 
  ├──
  └── ... 

# 项目有关配置文件
config/    
  ├── tasks/                           # 所有需运行任务有关的配置信息，以pipeline的形式定义
      ├──
      └──  ...
  └── entrance/                        # 初始化调度器所需要的pipeline配置
      ├──
      └──  ...
  └── resource/                        # 资源有关的配置
      ├──
      └──  ...                             

# 任务函数
tasks/
  ├── market_data/                     # 股票金融实时数据
      ├──
      └──  ...
  ├── news_data/                       # 新闻舆情
      ├──
      └──  ...
  ├── traditional_analysis/            # 数据库查询+规则分析
      ├──
      └──  ...
  ├── ai_agent_analysis/               # AI/Agent智能分析
      ├──
      └──  ...

# web有关函数
web/
  ├── static/                          # css与js
      ├── apiService.js                # 用于网页与后端通信
      ├── constants.js                 # 网页所需常量
      ├── eventHandlers.js             # 用于事件处理
      ├── render.js                    # 用于界面动态渲染
      ├── script.js                    # 主函数，用于DOM获取与基础事件绑定
      ├── util.js                      # 一些工具
      ├── style.css                    # 定义了html中每个元素的风格
      └──  ...
  ├── templates/                       # 网页
      └──  index.html                  # 主入口
  ├── util/                            # 一些后端工具
      └──  pipeline_conf_manager       # 用于管理pipeline配置

# 启动应用程序
webapp_main.py                                
```

* 数据库当前的 ER 图
  ![er_diagram](cache/er_diagram.png)



# 2. 环境配置

**Python环境**

* Linux系统使用venv构建虚拟环境（同时可使用系统环境或conda环境）
  ````cmd
  # 创建虚拟环境（可用conda）
  python3 -m venv ashare_env
  
  # 激活虚拟环境
  source /home/Ashare/ashare_env/bin/activate

  # 下载依赖（其中数据库有关操作可不配置，通过让config/entrance或tasks中pipeline配置的"save": 2即可令数据存储到csv中，对应存储逻辑可在tasknode中找到）
  pip install -r requirements.txt

  # 运行
  python webapp_main.py
  ````

**postgresql**

* 参考文档: https://docs.sqlalchemy.org/en/21/tutorial/data_insert.html\
* **注意**: sql查询语句中输入字段名时，是未指定大小写的，若表名为大写，则需要使用双引号框起来；写值时同理，为数据直接写，为字符串需要加引号

1. 官网下载数据库，安装，并将bin文件夹添加到环境变量 -- **执行时千万要记住数据库操作是要＋“;”的**
1. `sudo -i -u postgres`登录到postsql用户；`exit`退出到普通用户（该用户模式下可使用psql指令）；登陆后`psql`进入数据库终端(linux)
2. `CREATE USER <username> WITH PASSWORD '<password>';`创建用户(linux可直接与home下的user同名)
3. `psql -U <username> -d <target_db>;` 连接到对应数据库(当在linux中将db所有者设置为用户名时，可以直接`psql -d <target_db>`登录)
4. `CREATE DATABASE <db_name> OWNER <username>;`创建数据库
5. `CREATE EXTENSION vector;`在对应数据库中启用vector插件
6. `DROP TABLE IF EXISTS <target_chart>;`删除掉对应的表, `\dt`查看当前数据库中的表
7. `\d <target_table>` 指令可用于查看表中所有列的名称, `SELECT COUNT(*) AS total_rows FROM <target_table>;` 查看表中元素数量
8. `SET CLIENT_ENCODING TO 'utf8';` 客户端终端编码进行设置为 UTF-8 模式
9.  向量数据库有关支持配置方式（参考：https://github.com/pgvector/pgvector#）
   ```cmd
   # linux：
   cd /tmp
   git clone --branch v0.8.0 https://github.com/pgvector/pgvector.git
   cd pgvector
   sudo make
   sudo make install
   
   
   # Windows：（需确保vs studio以安装，要用里面提供的nmake）
   call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat"  # 添加环境变量
   set "PGROOT=C:\Program Files\PostgreSQL\16"
   cd %TEMP%
   git clone --branch v0.8.0 https://github.com/pgvector/pgvector.git
   cd pgvector
   nmake /F Makefile.win
   nmake /F Makefile.win install
   ```

10. alembic自动数据库格式检测(列类名的转化可能有bug)
   ```cmd
   # 初始化alembic工作文件夹
   alembic init <alembic_work_folder>
   
   # 配置<alembic_work_folder>中env.py文件（导入数据库模板ORM）
   from stock_database import Base
   target_metadata = Base.metadata
   
   # 配置alembic.ini中的工作目录
   script_location = <alembic_work_folder>
   
   # 配置alembic.ini中的数据库url
   sqlalchemy.url = postgresql+psycopg2://<user>:<password>@<host>:<port>/<database>
   
   # 对比ORM中定义的表与数据库中已有的表之间的区别，并生成一个用于更新record（可检测是否有表的新增，或表中类的修改）
   alembic revision --autogenerate -m "check table consistency"
   
   # 运行指令将数据库中的表按照 ORM 中定义进行更新
   alembic upgrade head
   
   # 当迭代py文件被手动删除时直接drop table alembic_version
   ```

11. 数据库的session与connection

    * connection是低级数据库连接，特性包括：即时执行：所有SQL会立即发送到数据库;无状态跟踪：不维护ORM对象状态;轻量级：相比session开销更小;需要手动管理事务（除非使用with块自动提交）
    * session与connection不同，session是高级抽象层，特性包括：操作注册：ORM变更会被注册到session中，但不会立即执行SQL;事务管理：通过session.commit()统一提交所有挂起操作;身份映射：维护对象唯一性，避免重复加载同一数据;自动脏检查：跟踪对象状态变化

    

**longchain使用**

* longchain作用：为模型配置调用类，并提供一些操作

* aliyun api (不好用，换成chatanywhere)

  * 官方网址：https://bailian.console.aliyun.com/

  * aliyun官方longchain使用教程：https://help.aliyun.com/zh/model-studio/use-bailian-in-langchain
* chatanywhere api
  * 官网网址：https://api.chatanywhere.tech/



**apache-airflow(弃用)**

* 作用: 工作流管理与调度
* 官方文档: https://airflow-doc-zh.readthedocs.io/zh-cn/latest/5/

* 配置方法(对window兼容性奇差无比)
  ```cmd
  # 在bashrc中配置export AIRFLOW_HOME
  sudo vim ~/.bashrc
  export AIRFLOW_HOME=/home/yelu/Ashare/core/scheduler
  source ~/.bashrc
  
  # 在airflow_home中初始化基础配置文件
  airflow db migrate
  
  # 运行airflow
  airflow standalone
  ```



# 3. 项目使用

**(0) 项目核心的逻辑**

* 参考：apache airflow
* 目的：该项目希望能设计一个全自动的金融市场分析软件，并提供较为良好的扩展性。为实现该目标，本项目设计了一个任内务调度系统，用于管理项目各个任务的执行。
* 框架设计：该程序的核心为一个调度器类`Scheduler`，其负责管理所有任务的执行。该项目的执行单元为`Pipeline`类，其有一个唯一的`PipelineExecutor`调度器进行管理。`Scheduler`负责`Pipeline`与`PipelineExecutor`的管理，任务包括注册，启动，重启与终止。`Pipeline`类为workflow的配置文件，其会通过对应的配置json来初始化。`Pipeline`初始化的过程中会实例化最小的执行单元 `Task`类 并加载其在workflow中的依赖关系。每一个实例化的`Pipeline`对应了一个`PipelineExecutor`，其负责按照依赖关系执行`Pipeline`中全部的task，包含task的调度，注册，执行，重启等。此外，`PipelineExecutor`也支持通过`Task`的输出动态注册新的`Task`或`Pipeline`，但是`Task`需按照规范返回对应格式的result。
* 设计原则：
  1. 每一层任务的注册，生命周期的管理均由其上一层调度，如task的运行由pipeline executor调度，而pipeline则由scheduler直接调度（如scheduer类缓存需运行的pipeline与对应executor字典，pipelien executor存储task实例与依赖关系图）。当前层的类则需要提供对应的函数，用于其上层调度类对其状态的查询
  2. 有关于pipeline与pipelineExecutor职责的划分，pipeline类用于资源管理，用于初始化任务与任务流程并管理任务图；而pipelineExecutor是一个调度器，其用于访问pipelien中的信息，并执行调度的功能。
  3. 有关动态注册的pipeline，其会将对应的请求放到队列中，scheduler会监控这些请求。


**(1) 创建一个新的task**

* 说明：该步骤用于新建一个任务，其为pipeline的执行单元，其格式符合以下形式
  ```
  def example_task(resource_config, task_params):
      try:
          db_conn = resource_manager.get("postgres_db")
          result = db_conn.query("SELECT * FROM stocks;")  # 伪代码
          return {"status": "success", "data": result, "error": None}
  
      except Exception as e:
          return {"status": "failed", "data": None, "error": str(e)}
  ```

  * 其传入参数为resource_config(资源配置文件), task_params(项目执行所需参数)
  * 其输出为json{"status": "success", "data": result, "error": None}，对应任务执行状态，任务输出，错误信息
  * 其输出包含可选项 "next_task" / "next_pipeline"， 用于动态出发task或是pipeline
  * task由try-catch包裹，以防止其影响当前线程，task应保证面对各种类型的错误输入，不会产生未知的运行错误，同时将产生的异常正确的输出！！

* 构建方式

  1. resource_config: task函数将在其内部通过resource_manager中的factory方法与resource_config创建其运行所需的资源，包括agent实例，数据库会话等等，其为该任务运行所必需的参数，同时不会由于workflow的改变而改变
  2. task_params: json文件，其参数在不同workflow中会有所不同。workflow配置文件会定义task中所必需的参数，同时会定义动态参数的依赖关系（该参数为当前task所依赖的task的输出），task函数不会考虑对应的依赖关系，但会判断所需参数是否完整。
  3. 函数输出：默认情况下，函数会将其运行结果存放于dataframe中并放置在"data"中。但如果当前task涉及到关系型数据库或向量数据库的操作，其运行结果可能会优先存放于数据库中，后续的task通过数据库获取对应所需的信息。



**(2) 创建一个新的workflow**

* 样例
  ```json
  {
      "name": "test_pipeline",
      "frequency": 1,
      "boot_time": null,
      "tasks": [
        {
          "name": "get_info1",
          "func_name": "fetch_bing_news",
          "params": {
            "query": "中国 财经 新闻",
            "max_results": 15,
            "whitelist": [
                "finance.sina.com.cn",
                "cn.reuters.com",
              ]
          },
          "retry_interval": 10,
          "dependencies": {}
        },
        {
          "name": "analyze",
          "func_name": "fetch_ali_llm_chat",
          "params": {
            "use_long_term_memory": false,
            "vector_database_index": "",
            "user_prompt": "你将作为我的新闻分析员。下面为我想你提供的若干条财经新闻，请你选出其中对中国金融市场有影响的新闻，并基于其内容，提取关键信息，并将新闻摘要返回给我",
            "net_search_prompt": ""
          },
          "retry_interval": 10,
          "dependencies": {
            "get_info1": "net_search_prompt",
          }
        }
      ]
    }
  ```

  * workflow: 该json定义了pipeline的基本数据，如名称，重启次数，启动逻辑等等，同时其定义了该pipeline中所涉及到的tasks的执行顺序与task之间的依赖关系
  * tasks: task的配置文件，其中"name"为该task在相应workflow实例中的唯一标识符；"func_name"为该task所调用函数的名称，用于函数的动态加载；"params"为当前函数运行所需要的参数（task函数的输入参数中的task_params）；"retry_interval"等参数定义了函数的运行逻辑；"dependencies"参数为一个字典，其定义了该函数在当前pipeline中所依赖的函数，并定义了当前函数传入参数与其依赖函数间的对应关系（"get_info1": "net_search_prompt" -> 当前函数须在"get_info1"执行完后再执行，当前函数params中"net_search_prompt"的值为"get_info1"的输出）



# 4. 一些笔记

**github**

* .gitignore文件用于设置哪些文件不会被git记录
  ```
  *.txt  ，*.xls  表示过滤某种类型的文件
  target/ ：表示过滤这个文件夹下的所有文件
  /test/a.txt ，/test/b.xls  表示指定过滤某个文件下具体文件
  !*.java , !/dir/test/     !开头表示不过滤
  *.[ab]    支持通配符：过滤所有以.a或者.b为扩展名的文件
  /test  仅仅忽略项目根目录下的 test 文件，不包括 child/test等非根目录的test目录
  ```

* 项目更新提交流程
  ```cmd
  git add . 
  git commit -m "describe revision"
  git push origin <your branch>
  ```

* 一些相关操作
  ```cmd
  # 新建远程分支
  git checkout -b <新分支名称>

  # 更改本地分支名称为<target>
  git branch -m <target>
  
  # 删除远程分支<target>
  git push origin --delete <target>

  # 清除git缓存
  git rm -r --cached .

  # 查看git追踪文件
  git status
  ```

* 添加github ssh密钥：https://blog.csdn.net/weixin_42310154/article/details/118340458

* 检验push错误
  ```
  # 解析ssh github被解析的ip地址，若为本地地址则一定有问题！github更新后ssh无法通过密码登陆
  ssh -T git@github.com

  # 当github host地址更新导致报错时，清除本地knownhost缓存（此处建议国内直接绑定github的网址）
  ssh-keygen -R github.com

  # 1.解析为本地地址可尝试更换DNS服务器，如：
  8.8.8.8

  # 2.国内的github ipv4地址不稳定，可能使用不了，需修改 C://windows/system32/drivers/etc/hosts 文件手动DNS解析
  # 以下为英国地址，要挂梯子
  20.26.156.215 github.com 

  # 可用https://dnschecker.org/查看github.com在全球的ip地址，以修改DNS解析
  ```

* git缓存一直有问题 -- 重制仓库
  ```
  rm -rf .git
  git init
  git add .
  git commit -m "初始化干净版本"
  # 关联仓库
  git remote add origin git@github.com:your-username/your-repo.git
  # 新建分支
  git branch -M ubuntu
  git push -u origin ubuntu
  ```

**notice**

* vscode在debug时会以当前文件所在文件夹作为根目录进行相关自定义的package的查找，所以会产生找不到对应package的报错
  ```
  # 通过设置launch.json文件，并在debug时通过launch文件启动。以下内容设定debug时会在项目根目录处执行
  "cwd": "${workspaceFolder}",
  "env":{"PYTHONPATH":"${workspaceRoot}"},
  ```

**ubuntu**

* 参考链接：https://blog.csdn.net/jjj_web/article/details/147342382
* 新建账户与权限管理
  ```
  # 新建账户，包含个人信息 + 密码设置
  sudo adduser username

  # 将新用户加入soduer组
  sudo usermod -aG sudo newuser

  # 为root用户设置密码(建议不要设置)
  sudo passwd root

  # 禁用 / 启用root账户密码
  sudo passwd -l root
  sudo passwd -u root

  # 启禁用ssh root登陆
  sudo nano /etc/ssh/sshd_config
  PermitRootLogin no

  # 查看用户用户组
  groups username
  ```

  

