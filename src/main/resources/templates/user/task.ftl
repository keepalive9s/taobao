<!DOCTYPE html>
<html lang="en">
<head>
    <title>任务管理</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="stylesheet" href="/taoshelf/static/css/bootstrap.css">
    <link rel="stylesheet" href="https://unpkg.com/element-ui/lib/theme-chalk/index.css">
    <style>
        th, td {
            white-space: nowrap;
            text-align: center;
        }
    </style>
    <script src="/taoshelf/static/js/jquery.js"></script>
    <script src="/taoshelf/static/js/bootstrap.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/vue@2.5.17/dist/vue.min.js"></script>
    <script src="https://unpkg.com/element-ui@2.4.8/lib/index.js"></script>
</head>
<body>
<nav class="navbar bg-dark navbar-expand-md navbar-dark">
    <a class="navbar-brand" href="/taoshelf/home">有梦循环上下架</a>
    <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#collapsibleNavbar">
        <span class="navbar-toggler-icon"></span>
    </button>
    <div class="collapse navbar-collapse" id="collapsibleNavbar">
        <ul class="navbar-nav">
            <li class="nav-item">
                <a class="nav-link" href="/taoshelf/home">首页</a>
            </li>
            <li class="nav-item">
                <a class="nav-link active" href="/taoshelf/task">任务管理</a>
            </li>
            <li class="nav-item">
                <a class="nav-link" href="/taoshelf/log">日志信息</a>
            </li>
            <li class="nav-item dropdown">
                <a class="nav-link dropdown-toggle" href="#" id="navbardrop" data-toggle="dropdown">
                    账户
                </a>
                <div class="dropdown-menu">
                    <a class="dropdown-item" href="#">${user.nick}</a>
                    <a class="dropdown-item" href="#">到期时间：${user.endTime}</a>
                    <div class="dropdown-divider"></div>
                    <a class="dropdown-item" href="/taoshelf/recharge">账户充值</a>
                </div>
            </li>
        </ul>
    </div>
</nav>
<div class="container-fluid" id="app">
    <div class="row" style="margin-top: 15px">
        <h2 class="col-6">任务列表</h2>
        <div class="col-6">
            <a href="/taoshelf/task1_add" class="btn btn-primary float-right">创建任务</a>
        </div>
    </div>
    <#if info?exists>
        <#if info.type="success">
            <div class="alert alert-success alert-dismissable">
                <button type="button" class="close" data-dismiss="alert">&times;</button>
                <strong>提示!</strong> ${info.message}
            </div>
        <#elseif info.type="error">
            <div class="alert alert-danger alert-dismissable">
                <button type="button" class="close" data-dismiss="alert">&times;</button>
                <strong>错误!</strong> ${info.message}
            </div>
        </#if>
    </#if>
    <div class="table-responsive">
        <table class="table table-hover table-sm">
            <thead>
            <tr>
                <th>#</th>
                <th>描述</th>
                <th>数量</th>
                <th>开始时间</th>
                <th>结束时间</th>
                <th>状态</th>
                <th>操作</th>
            </tr>
            </thead>
            <tbody>
            <#list tasks as task>
            <tr>
                <td>${task_index+1}</td>
                <td>${task.description!}</td>
                <td>${task.num!}</td>
                <td>${task.startTime!}</td>
                <td>${task.endTime!}</td>
                <td>${task.status!}</td>
                <td>
                    <button class="btn btn-danger btn-sm" onclick="stoptask('${task.id}')">中止</button>
                    <a href="/taoshelf/remove_task?id=${task.id}" class="btn btn-danger btn-sm">删除</a>
                </td>
            </tr>
            </#list>
            </tbody>
        </table>
        <el-pagination @size-change="handleSizeChange"
                       @current-change="handleCurrentChange"
                       :current-page="currentPage"
                       :page-sizes="[8,20,50,100]"
                       :page-size="pageSize"
                       layout="total, sizes, prev, pager, next, jumper"
                       :total="${task_total?c}">
        </el-pagination>
        <p style="margin-bottom: 15px"></p>
    </div>
</div>
</body>
<script>
    function stoptask(id) {
        $.ajax({
            type: 'post',
            url: "/taoshelf/stop_task",
            data: {
                "id": id
            },
            beforeSend: function () {
                return confirm("确认中止？");
            },
            success: function (response) {
                alert(response);
                location.reload();
            }
        })
    }
</script>
<script>
    new Vue({
        el: '#app',
        methods: {
            handleSizeChange(val) {
                window.location.href = "/taoshelf/task?page_no=" + this.currentPage + "&page_size=" + val;
            },
            handleCurrentChange(val) {
                window.location.href = "/taoshelf/task?page_no=" + val + "&page_size=" + this.pageSize;
            }
        },
        data() {
            return {
                currentPage: ${page_no?c},
                pageSize: ${page_size?c}
            };
        }
    })
</script>
</html>