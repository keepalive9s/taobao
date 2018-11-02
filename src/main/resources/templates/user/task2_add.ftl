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
            <li class="nav-item active">
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
<div class="container" id="app">
    <form method="post" action="/taoshelf/add_task2">
        <div class="row" style="margin-top: 15px">
            <h2 class="col-6">配置定时任务(两种方式请选择一种)</h2>
            <div class="col-6">
                <button type="submit" class="btn btn-success float-right">配置好了，提交任务</button>
            </div>
        </div>
        <div class="row" style="margin-top: 15px">
            <div class="col-12">
                <ul class="nav nav-tabs">
                    <li class="nav-item">
                        <a href="/taoshelf/task1_add" class="nav-link">循环上下架</a>
                    </li>
                    <li class="nav-item">
                        <a href="/taoshelf/task2_add" class="nav-link active">完整上下架</a>
                    </li>
                </ul>
            </div>
        </div>
        <div class="row" style="margin-top: 15px">
            <div class="col-12">
                <div class="alert alert-success alert-dismissable">
                    <button type="button" class="close" data-dismiss="alert">&times;</button>
                    <strong>完整上下架</strong> ：指定任务开始时间，系统将对用户在售或库存商品进行一次完整上下架，不能有正在执行或待执行的其他任务在本任务的开始时间之后，一次只能设置一个完整上下架任务
                </div>
            </div>
        </div>
        <div class="row" style="margin-top: 15px">
            <h5 class="col-12">1、选择要处理的商品：</h5>
            <label class="col-6">
                <select class="form-control" name="type" required>
                    <option value="在售商品">在售商品</option>
                    <option value="仓库商品">仓库商品</option>
                </select>
            </label>
        </div>
        <div class="row" style="margin-top: 15px">
            <h5 class="col-12">2、设置任务执行时间：<span
                    style="font-size: small;color: red">*不能与已有任务的时间冲突</span></h5>
            <label class="col-6">开始时间
                <el-date-picker v-model="value1" type="datetime" placeholder="选择日期时间" :value-format='p'>
                </el-date-picker>
                <input type="text" name="start" class="form-control" required v-model:value="value1" hidden>
            </label>
        </div>
    </form>
</div>
</body>
<script>
    new Vue({
        el: '#app',
        data: function () {
            return {
                visible: false,
                value1: '',
                value2: '',
                p: 'yyyy-MM-dd HH:mm:ss'
            }
        }
    })
</script>
</html>