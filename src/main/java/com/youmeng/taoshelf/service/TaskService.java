package com.youmeng.taoshelf.service;

import com.youmeng.taoshelf.entity.PageInfo;
import com.youmeng.taoshelf.entity.Task;
import com.youmeng.taoshelf.entity.User;
import com.youmeng.taoshelf.quartz.Job2;
import com.youmeng.taoshelf.quartz.Job1;
import com.youmeng.taoshelf.repository.TaskRepository;
import org.quartz.*;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import javax.annotation.Resource;
import javax.transaction.Transactional;
import java.util.Date;
import java.util.List;

@Transactional
@Service
public class TaskService {

    @Resource
    private Scheduler scheduler;

    @Resource
    private TaskRepository taskRepository;

    public PageInfo addTask(Task task) {
        if (task.getEndTime() != null) {
            if (task.getStartTime().after(task.getEndTime())) {
                return new PageInfo("error", "创建任务失败，任务开始时间不能晚于结束时间");
            }
            if (task.getEndTime().after(task.getUser().getEndTime())) {
                return new PageInfo("error", "创建任务失败，任务结束时间不能晚于账户到期时间，请及时充值");
            }
            List<Task> tasks = getTasksByUser(task.getUser());
            for (Task item : tasks) {
                Date startTime1 = item.getStartTime();
                Date endTime1 = item.getEndTime();
                if (item.getDescription().contains("完整上下架")) {
                    return new PageInfo("error", "创建任务失败，有完整上下架任务还未完成");
                }
                if (startTime1.before(task.getEndTime()) && endTime1.after(task.getStartTime())) {
                    return new PageInfo("error", "创建任务失败，不得与已有任务时间段重叠");
                }
            }
        } else {
            List<Task> tasks = getTasksByUser(task.getUser());
            for (Task item : tasks) {
                if (item.getStatus().contains("正在执行") || item.getStatus().contains("等待执行")) {
                    return new PageInfo("error", "创建任务失败，完整上下架任务时不得有其他正在执行或等待执行的任务");
                }
            }
        }
        User user = task.getUser();
        try {
            taskRepository.save(task);
            String name = task.getId();
            String group = user.getNick();
            JobDetail jobDetail;
            if (task.getEndTime() != null) {
                jobDetail = JobBuilder
                        .newJob(Job1.class)
                        .withIdentity(name, group)
                        .usingJobData("task_id", task.getId())
                        .build();
            } else {
                jobDetail = JobBuilder
                        .newJob(Job2.class)
                        .withIdentity(name, group)
                        .usingJobData("task_id", task.getId())
                        .build();
            }
            Trigger trigger = TriggerBuilder
                    .newTrigger()
                    .withIdentity(name, group)
                    .startAt(task.getStartTime())
                    .build();
            scheduler.scheduleJob(jobDetail, trigger);
            task.setStatus("等待执行");
            taskRepository.save(task);
            return new PageInfo("success", "创建任务成功");
        } catch (Exception e) {
            e.printStackTrace();
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new PageInfo("error", "创建任务失败");
        }
    }

    public PageInfo removeTaskById(User user, String id) {
        Task task = taskRepository.findTaskById(id);
        if (task == null) {
            return new PageInfo("error", "删除任务失败，非法操作");
        } else if (!task.getUser().getNick().equals(user.getNick())) {
            return new PageInfo("error", "删除任务失败，非法操作");
        } else if (task.getStatus().contains("正在执行")) {
            return new PageInfo("error", "删除任务失败，任务正在进行请先中止");
        }
        try {
            taskRepository.delete(task);
            scheduler.deleteJob(JobKey.jobKey(task.getUser().getNick(), id));
            return new PageInfo("success", "删除任务成功");
        } catch (Exception e) {
            e.printStackTrace();
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new PageInfo("error", "删除任务失败");
        }
    }

    public Page<Task> getTasksByUser(User user, int no, int size) {
        Sort sort = new Sort(Sort.Direction.DESC, "createTime");
        Pageable pageable = PageRequest.of(no, size, sort);
        return taskRepository.findTasksByUser(user, pageable);
    }

    private List<Task> getTasksByUser(User user) {
        return taskRepository.findTasksByUser(user);
    }

    @CacheEvict(value = "task")
    public String stopTaskById(String id) {
        Task task = taskRepository.findTaskById(id);
        if (task.getStatus().equals("等待执行")) {
            return "任务还未开始，不能中止";
        } else if (task.getStatus().contains("执行完毕")) {
            return "任务已结束，不能中止";
        } else {
            try {
                task.setEndTime(new Date());
                taskRepository.save(task);
                return "任务中止成功，正在恢复商品初始状态请等待";
            } catch (Exception e) {
                e.printStackTrace();
                TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
                return "任务中止失败";
            }
        }
    }

    @Cacheable(value = "task")
    public Task findTaskById(String task_id) {
        return taskRepository.findTaskById(task_id);
    }

    @CachePut(value = "task")
    public Task getTaskById(String task_id) {
        return taskRepository.findTaskById(task_id);
    }

    public void saveTask(Task task) {
        taskRepository.save(task);
    }

    //获取所有正在执行任务
    public Page<Task> getAllTasksByStatus(String s, int no, int size) {
        Pageable pageable = PageRequest.of(no, size);
        return taskRepository.findTasksByStatusContains(s, pageable);
    }
}
