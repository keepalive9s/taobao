package com.youmeng.taoshelf.quartz;

import com.youmeng.taoshelf.entity.Good;
import com.youmeng.taoshelf.entity.Result;
import com.youmeng.taoshelf.entity.Task;
import com.youmeng.taoshelf.entity.User;
import com.youmeng.taoshelf.service.GoodService;
import com.youmeng.taoshelf.service.LogService;
import com.youmeng.taoshelf.service.TaskService;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.quartz.QuartzJobBean;

import javax.annotation.Resource;
import java.util.*;

public class Job2 extends QuartzJobBean {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    private TaskService taskService;

    @Resource
    private GoodService goodService;

    @Resource
    private LogService logService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private User user;

    private String taskId;

    private Task task;

    private long totalNum;

    private ArrayDeque<Good> goodQueue1 = new ArrayDeque<>();

    private ArrayDeque<Good> goodQueue2 = new ArrayDeque<>();

    private List<Good> failList1 = new ArrayList<>();

    private List<Good> failList2 = new ArrayList<>();

    private int busyCount1;

    private int busyCount2;

    private boolean finish1 = false;

    private boolean finish2 = false;

    @Override
    protected void executeInternal(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobDetail jobDetail = jobExecutionContext.getJobDetail();
        JobDataMap jobDataMap = jobDetail.getJobDataMap();
        //从context中获取task_id、user
        taskId = jobDataMap.getString("task_id");
        redisTemplate.opsForValue().set(taskId, "0");
        task = taskService.getTaskById(taskId);
        user = task.getUser();
        startTask();
    }

    //开始任务
    private void startTask() {
        logService.log(user, task.getDescription() + "任务进度", "任务开始");
        readTotalNum(task.getType());
        logService.log(user, task.getDescription() + "任务进度", "任务总数" + totalNum + "件");
        task.setNum(totalNum);
        task.setStatus("正在读取商品列表");
        taskService.saveTask(task);
        task = taskService.getTaskById(taskId);
        String s1 = "1-" + totalNum / 400;
        String s2 = totalNum / 400 + 1 + "-" + (totalNum / 200 + 1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    logService.log(user, task.getDescription() + "任务进度", "子任务2开始执行");
                    readGoodList(goodQueue2, s2, task.getType(), 2);
                    executeGoodList(goodQueue2, 2);
                    recoveryGoodList(failList2, 2);
                    logService.log(user, task.getDescription() + "任务进度", "子任务2执行完毕");
                    finish2 = true;
                } catch (Exception e) {
                    e.printStackTrace();
                    logService.log(user, task.getDescription() + "任务进度", "子任务2异常中止");
                    recoveryGoodList(failList2, 2);
                    finish2 = true;
                }
                if (finish1) {
                    task.setEndTime(new Date());
                    task.setStatus("任务结束(成功处理" + redisTemplate.opsForValue().get(taskId) + "次)");
                    taskService.saveTask(task);
                    redisTemplate.delete(taskId);
                }
            }
        }).start();
        try {
            logService.log(user, task.getDescription() + "任务进度", "子任务1开始执行");
            readGoodList(goodQueue1, s1, task.getType(), 1);
            executeGoodList(goodQueue1, 1);
            recoveryGoodList(failList1, 1);
            logService.log(user, task.getDescription() + "任务进度", "子任务1执行完毕");
            finish1 = true;
        } catch (Exception e) {
            e.printStackTrace();
            logService.log(user, task.getDescription() + "任务进度", "子任务1异常中止");
            recoveryGoodList(failList1, 1);
            finish1 = true;
        }
        if (finish2) {
            task.setEndTime(new Date());
            task.setStatus("任务结束(成功处理" + redisTemplate.opsForValue().get(taskId) + "次)");
            taskService.saveTask(task);
            redisTemplate.delete(taskId);
        }
    }

    //读总数(单线程)
    private void readTotalNum(String type) {
        switch (type) {
            case "仓库商品": {
                Result<Good> result = goodService.getGoodsInstock(user, null, 5L, 1L, 1);
                totalNum = result.getTotal();
                break;
            }
            case "在售商品": {
                Result<Good> result = goodService.getGoodsOnsale(user, null, 5L, 1L, 1);
                totalNum = result.getTotal();
                break;
            }
        }
    }

    //读列表(双线程)
    private void readGoodList(ArrayDeque<Good> goodQueue, String page, String type, int flag) {
        String[] split = page.split("-");
        long start = Long.parseLong(split[0]);
        long end = Long.parseLong(split[1]);
        switch (type) {
            case "仓库商品": {
                for (long i = start; i <= end; i++) {
                    Result<Good> result = goodService.getGoodsInstock(user, null, 200L, i, flag);
                    goodQueue.addAll(result.getItems());
                }
                break;
            }
            case "在售商品": {
                for (long i = start; i <= end; i++) {
                    Result<Good> result = goodService.getGoodsOnsale(user, null, 200L, i, flag);
                    goodQueue.addAll(result.getItems());
                }
                break;
            }
        }
        logService.log(user, task.getDescription() + "任务进度", "子任务" + flag + "分配" + goodQueue.size() + "件");
        task.setStatus("正在执行任务");
        taskService.saveTask(task);
        task = taskService.getTaskById(taskId);
    }

    //处理商品列表
    private void executeGoodList(ArrayDeque<Good> goodQueue, int flag) {
        while (!goodQueue.isEmpty()) {
            Good good = goodQueue.pop();
            task = taskService.findTaskById(taskId);
            if (task.getEndTime() != null && task.getEndTime().before(new Date())) {
                break;
            }
            task = taskService.findTaskById(taskId);
            if (executeGood(good, flag)) {
                if (executeGood(good, flag)) {
                    //成功
                    redisTemplate.opsForValue().increment(taskId, 1);
                    logger.info(user.getNick() + ":" + flag);
                } else {
                    //失败则加入再处理队
                    fail(flag, good);
                }
            } else {
                //失败则补到队尾
                goodQueue.add(good);
            }
        }
    }

    //处理一件商品
    private boolean executeGood(Good good, int flag) {
        rest(flag);
        if (good.getApproveStatus().equals("onsale")) {
            if (goodService.doGoodDelisting(user, good, flag)) {
                notBusy(flag);
                return true;
            } else {
                busy(flag);
            }
        } else if (good.getApproveStatus().equals("instock")) {
            if (goodService.doGoodListing(user, good, flag)) {
                notBusy(flag);
                return true;
            } else {
                busy(flag);
            }
        }
        return false;
    }

    //恢复商品列表
    private void recoveryGoodList(List<Good> goodList, int flag) {
        logService.log(user, task.getDescription() + "任务进度", "开始恢复子任务" + flag + "(" + goodList.size() + "件)");
        for (Good good : goodList) {
            int count = 0;
            while (count < 2) {
                if (executeGood(good, flag)) {
                    redisTemplate.opsForValue().increment(taskId, flag);
                    logService.log(user, "恢复商品原状态成功", good.getTitle());
                    break;
                } else {
                    count++;
                }
            }
            if (count == 2) {
                logService.log(user, "恢复商品原状态失败", good.getTitle());
            }
        }
        logService.log(user, task.getDescription() + "任务进度", "恢复子任务" + flag + "(" + goodList.size() + "件)完成");
    }

    private void notBusy(int flag) {
        if (flag == 1) {
            busyCount1 = 0;
        } else {
            busyCount2 = 0;
        }
    }

    private void busy(int flag) {
        if (flag == 1) {
            busyCount1++;
        } else {
            busyCount2++;
        }
    }

    private void rest(int flag) {
        int count;
        int sleepTime;
        if (flag == 1) {
            count = busyCount1;
        } else {
            count = busyCount2;
        }
        if (count == 0) {
            sleepTime = 400;
        } else if (count == 1) {
            sleepTime = 2500;
        } else if (count == 2) {
            sleepTime = 4000;
        } else {
            sleepTime = 6000;
        }
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void fail(int flag, Good good) {
        if (flag == 1) {
            failList1.add(good);
        } else {
            failList2.add(good);
        }
    }
}
