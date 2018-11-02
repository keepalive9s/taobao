<!DOCTYPE html>
<html lang="en">
<head>
    <title>首页</title>
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
                <a class="nav-link active" href="/taoshelf/home">首页</a>
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
                    <a class="dropdown-item" href="#">使用到期时间：${user.endTime}</a>
                    <div class="dropdown-divider"></div>
                    <a class="dropdown-item" href="/taoshelf/recharge">账户充值</a>
                </div>
            </li>
        </ul>
    </div>
</nav>
<div class="container-fluid">
    <div class="row">
        <div class="col-lg-4 col-md-3 col-sm-1"></div>
        <div class="col-lg-4 col-md-6 col-sm-10" style="text-align: center">
            <img src="/taoshelf/static/img/logo.jpg" class="img-fluid">
            <form action="/taoshelf/result" method="get">
                <div class="row">
                    <select name="type" class="col-md-2 col-sm-4 form-control" style="display: inline-block">
                        <option value="onsale">在售</option>
                        <option value="instock">库存</option>
                    </select>
                    <input name="q" type="text" class="col-md-10 col-sm-8 form-control" style="display: inline-block"
                           placeholder="请输入搜索关键字，为空则匹配所有">
                </div>
                <div class="row">
                    <div class="col-5"></div>
                    <input class="col-2 btn btn-outline-primary form-control" type="submit"
                           style="display: block;margin-top: 10px;"
                           value="搜索"/>
                    <div class="col-5"></div>
                </div>
            </form>
        </div>
        <div class="col-lg-4 col-md-3 col-sm-1"></div>
    </div>
    <div>
        <p>请注意应用授权使用到期时间，一旦到期将影响任务正常进行，请及时到服务市场续费</p>
        <p>服务使用到期时间：${user.endTime!}&nbsp;<a href="">充值</a></p>
        <p>应用1使用到期时间：${user.endDate1!}&nbsp;<a href="https://tb.cn/VdsUTMw" target="_blank">续费</a></p>
        <p>应用2使用到期时间：${user.endDate2!}&nbsp;<a href="https://tb.cn/QKThZNw" target="_blank">续费</a></p>
    </div>
</div>
</body>
</html>