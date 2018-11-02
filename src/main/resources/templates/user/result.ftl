<!DOCTYPE html>
<html lang="en">
<head>
    <title>搜索结果</title>
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
                <a class="nav-link" href="/taoshelf/task">任务管理</a>
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
    <div class="table-responsive">
        <table class="table table-hover table-sm">
            <thead>
            <tr>
                <th>#</th>
                <th>标题</th>
                <th>数量</th>
                <th>修改时间</th>
                <th>上架时间</th>
                <th>下架时间</th>
                <th>状态</th>
            </tr>
            </thead>
            <tbody>
            <#list goods as good>
            <tr>
                <td>${good_index+1}</td>
                <td>${good.title!}</td>
                <td>${good.num}</td>
                <td>${good.modified?string('yyyy-MM-dd HH:mm:ss')}</td>
                <td>${good.listTime?string('yyyy-MM-dd HH:mm:ss')}</td>
                <td>${good.delistTime?string('yyyy-MM-dd HH:mm:ss')}</td>
                <#if good.approveStatus?exists>
                    <#if good.approveStatus="instock">
                    <td>库存</td>
                    <#elseif good.approveStatus="onsale">
                    <td>在售</td>
                    </#if>
                <#else>
                <td>未知</td>
                </#if>
            </tr>
            </#list>
            </tbody>
        </table>
        <el-pagination
                @size-change="handleSizeChange"
                @current-change="handleCurrentChange"
                :current-page="currentPage"
                :page-sizes="[18 ,50 ,100, 200]"
                :page-size="pageSize"
                layout="total, sizes, prev, pager, next, jumper"
                :total="${result_total?c}">
        </el-pagination>
        <p></p>
    </div>
</div>
</body>
<script src="/taoshelf/static/js/jquery.js"></script>
<script src="/taoshelf/static/js/bootstrap.js"></script>
<script src="https://cdn.jsdelivr.net/npm/vue@2.5.17/dist/vue.min.js"></script>
<script src="https://unpkg.com/element-ui@2.4.8/lib/index.js"></script>
<script>
    new Vue({
        el: '#app',
        methods: {
            handleSizeChange(val) {
                window.location.href = "/taoshelf/result?q=${q!}&type=${type}&page_no=" + this.currentPage + "&page_size=" + val;
            },
            handleCurrentChange(val) {
                window.location.href = "/taoshelf/result?q=${q!}&type=${type}&page_no=" + val + "&page_size=" + this.pageSize;
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