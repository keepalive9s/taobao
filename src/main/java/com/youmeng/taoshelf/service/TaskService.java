package com.youmeng.taoshelf.service;

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

    public boolean addTask(Task task) {
        //TODO
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
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return false;
        }
    }

    public boolean removeTaskById(User user, String id) {
        Task task = taskRepository.findTaskById(id);
        if (task == null) {
            System.out.println("null");
            return false;
        } else if (!task.getUser().getNick().equals(user.getNick())) {
            return false;
        }
        try {
            taskRepository.delete(task);
            scheduler.deleteJob(JobKey.jobKey(task.getUser().getNick(), id));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return false;
        }
    }

    public Page<Task> getTasksByUser(User user, int no, int size) {
        Sort sort = new Sort(Sort.Direction.DESC, "createTime");
        Pageable pageable = PageRequest.of(no, size, sort);
        return taskRepository.findTasksByUser(user, pageable);
    }

    public List<Task> getTasksByUser(User user) {
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
