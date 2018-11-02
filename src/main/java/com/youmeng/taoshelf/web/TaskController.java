package com.youmeng.taoshelf.web;

import com.youmeng.taoshelf.entity.PageInfo;
import com.youmeng.taoshelf.entity.Task;
import com.youmeng.taoshelf.entity.User;
import com.youmeng.taoshelf.service.TaskService;
import com.youmeng.taoshelf.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Controller
public class TaskController {

    @Resource
    private TaskService taskService;

    @Resource
    private UserService userService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @RequestMapping("/task")
    public ModelAndView task(ModelMap modelMap, HttpSession session,
                             @RequestParam(defaultValue = "1") int page_no,
                             @RequestParam(defaultValue = "8") int page_size) {
        ModelAndView modelAndView = new ModelAndView("/user/task");
        User user = userService.getUserByNick((String) session.getAttribute("nick"));
        modelAndView.addObject("user", user);
        Page<Task> taskPage = taskService.getTasksByUser(user, page_no - 1, page_size);
        modelAndView.addObject("task_total", taskPage.getTotalElements());
        modelAndView.addObject("page_size", page_size);
        modelAndView.addObject("page_no", page_no);
        List<Task> tasks = taskPage.getContent();
        for (Task task : tasks) {
            if (task.getStatus().equals("正在执行任务")) {
                String count = redisTemplate.opsForValue().get(task.getId());
                task.setStatus(task.getStatus() + count + "次");
            }
        }
        modelAndView.addObject("tasks", tasks);
        if (modelMap.containsAttribute("info")) {
            PageInfo info = (PageInfo) modelMap.get("info");
            modelAndView.addObject("info", info);
        }
        return modelAndView;
    }

    @RequestMapping("/task1_add")
    public ModelAndView task1Add(HttpSession session) {
        ModelAndView modelAndView = new ModelAndView("/user/task1_add");
        User user = userService.getUserByNick((String) session.getAttribute("nick"));
        modelAndView.addObject("user", user);
        return modelAndView;
    }

    @RequestMapping("/task2_add")
    public ModelAndView task2Add(HttpSession session) {
        ModelAndView modelAndView = new ModelAndView("/user/task2_add");
        User user = userService.getUserByNick((String) session.getAttribute("nick"));
        modelAndView.addObject("user", user);
        return modelAndView;
    }

    @PostMapping("/add_task1")
    public String addTask1(HttpSession session, RedirectAttributes attributes,
                           @RequestParam String type,
                           @RequestParam String start,
                           @RequestParam String end) throws ParseException {
        User user = userService.getUserByNick((String) session.getAttribute("nick"));
        Task task = new Task();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date startTime = dateFormat.parse(start);
        Date endTime = dateFormat.parse(end);
        task.setType(type);
        task.setStartTime(startTime);
        task.setEndTime(endTime);
        task.setUser(user);
        if (taskService.addTask(task)) {
            attributes.addFlashAttribute("info", new PageInfo("success", "创建任务成功"));
            return "redirect:/task";
        } else {
            attributes.addFlashAttribute("info", new PageInfo("error", "创建任务失败"));
            return "redirect:/task";
        }
    }

    @PostMapping("/add_task2")
    public String addTask2(HttpSession session, RedirectAttributes attributes,
                           @RequestParam String type,
                           @RequestParam String start) throws ParseException {
        User user = userService.getUserByNick((String) session.getAttribute("nick"));
        Task task = new Task();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date startTime = dateFormat.parse(start);
        task.setType(type);
        task.setStartTime(startTime);
        task.setUser(user);
        if (taskService.addTask(task)) {
            attributes.addFlashAttribute("info", new PageInfo("success", "创建任务成功"));
            return "redirect:/task";
        } else {
            attributes.addFlashAttribute("info", new PageInfo("error", "创建任务失败"));
            return "redirect:/task";
        }
    }

    @GetMapping("/remove_task")
    public String removeTask(HttpSession session, RedirectAttributes attributes, @RequestParam String id) {
        User user = userService.getUserByNick((String) session.getAttribute("nick"));
        if (taskService.removeTaskById(user, id)) {
            attributes.addFlashAttribute("info", new PageInfo("success", "删除任务成功"));
            return "redirect:/task";
        } else {
            attributes.addFlashAttribute("info", new PageInfo("error", "删除任务失败"));
            return "redirect:/task";
        }
    }

    @RequestMapping("/stop_task")
    @ResponseBody
    public String stopTask(@RequestParam String id) {
        return taskService.stopTaskById(id);
    }
}
