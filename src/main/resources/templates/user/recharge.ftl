<!DOCTYPE html>
<html lang="en">
<head>
    <title>充值页面</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="stylesheet" href="/taoshelf/static/css/bootstrap.css">
    <style>
        th, td {
            white-space: nowrap;
            text-align: center;
        }
    </style>
    <script src="/taoshelf/static/js/jquery.js"></script>
    <script src="/taoshelf/static/js/bootstrap.js"></script>
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
            <li class="nav-item dropdown active">
                <a class="nav-link dropdown-toggle" href="#" id="navbardrop" data-toggle="dropdown">
                    账户
                </a>
                <div class="dropdown-menu">
                    <a class="dropdown-item" href="#">${user.nick}</a>
                    <a class="dropdown-item" href="#">到期时间：${user.endTime}</a>
                    <div class="dropdown-divider"></div>
                    <a class="dropdown-item active" href="/taoshelf/recharge">账户充值</a>
                </div>
            </li>
        </ul>
    </div>
</nav>
<div class="container">
    <div class="row" style="margin-top: 15px">
    </div>
    <div class="row">
        <div class="col-md-2"></div>
        <div class="col-md-8">
            <#if info?exists>
                <#if info.type="success">
            <div class="alert alert-info alert-dismissable">
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
            <h2>账户充值</h2>
            <form action="/taoshelf/submit_card_key" method="post">
                <div class="form-group">
                    <label for="key">输入购买获得的卡密以激活使用时长:</label>
                    <input type="text" class="form-control" name="key" autocomplete="off"
                           placeholder="请输入卡密"
                           required>
                </div>
                <input type="submit" class="form-control btn btn-primary" value="激活充值卡">
            </form>
            <br>
            <a href="" target="_blank">充值卡密购买地址</a><br>

        </div>
        <div class="col-md-2"></div>
    </div>
</div>
</body>
</html>