package com.youmeng.taoshelf.web;

import com.alibaba.fastjson.JSONArray;
import com.youmeng.taoshelf.entity.*;
import com.youmeng.taoshelf.service.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Controller
public class AdminController {

    @Resource
    private AdminService adminService;

    @Resource
    private UserService userService;

    @Resource
    private CardService cardService;

    @Resource
    private LogService logService;

    @Resource
    private TaskService taskService;

    @Resource
    private PasswordEncoder passwordEncoder;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @GetMapping("/8088/login")
    public ModelAndView login() {
        return new ModelAndView("/admin/login");
    }

    @RequestMapping("/8088/admin")
    public ModelAndView admin(@RequestParam(defaultValue = "1") int page_no,
                              @RequestParam(defaultValue = "15") int page_size) {
        ModelAndView modelAndView = new ModelAndView("/admin/admin");
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        Admin admin = adminService.findAdminByName(authentication.getName());
        Page<Task> taskPage = taskService.getAllTasksByStatus("正在执行", page_no - 1, page_size);
        modelAndView.addObject("task_total", taskPage.getTotalElements());
        modelAndView.addObject("page_size", page_size);
        modelAndView.addObject("page_no", page_no);
        List<Task> tasks = taskPage.getContent();
        for (Task task : tasks) {
            if (task.getStatus().equals("正在执行任务")) {
                String count = redisTemplate.opsForValue().get(task.getId());
                if (task.getEndTime() == null) {
                    task.setStatus(task.getStatus() + count + "/" + task.getNum());
                } else {
                    task.setStatus(task.getStatus() + "(第" + count + "次上下架)");
                }
            }
        }
        modelAndView.addObject("tasks", tasks);
        modelAndView.addObject("admin", admin);
        return modelAndView;
    }

    @RequestMapping("/8088/stop_task")
    @ResponseBody
    public String stopTask(@RequestParam String id) {
        return taskService.stopTaskById(id);
    }

    @RequestMapping("/8088/user")
    public ModelAndView user(@RequestParam(defaultValue = "") String s,
                             @RequestParam(defaultValue = "1") int page_no,
                             @RequestParam(defaultValue = "15") int page_size) {
        ModelAndView modelAndView = new ModelAndView("/admin/user");
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        Admin admin = adminService.findAdminByName(authentication.getName());
        modelAndView.addObject("admin", admin);
        Page<User> users = userService.getAllUsers(page_size, page_no - 1, s);
        List<User> userList = users.getContent();
        modelAndView.addObject("users", userList);
        modelAndView.addObject("card_total", users.getTotalElements());
        modelAndView.addObject("page_size", page_size);
        modelAndView.addObject("page_no", page_no);
        return modelAndView;
    }

    @PostMapping("/8088/del_user")
    @ResponseBody
    public String delUser(@RequestParam String nick) {
        Boolean aBoolean = userService.deleteUserByNick(nick);
        if (aBoolean) {
            return "success";
        } else {
            return "error";
        }
    }

    @GetMapping("/8088/card_new")
    public ModelAndView cardNew(@RequestParam(defaultValue = "1") int page_no,
                                @RequestParam(defaultValue = "15") int page_size) {
        ModelAndView modelAndView = new ModelAndView("/admin/card_new");
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        Admin admin = adminService.findAdminByName(authentication.getName());
        modelAndView.addObject("admin", admin);
        Page<Card> cards = cardService.getAllNewCard(page_size, page_no - 1);
        List<Card> cardList = cards.getContent();
        modelAndView.addObject("card_total", cards.getTotalElements());
        modelAndView.addObject("page_size", page_size);
        modelAndView.addObject("page_no", page_no);
        modelAndView.addObject("cards", cardList);
        return modelAndView;
    }

    @GetMapping("/8088/card_old")
    public ModelAndView cardOld(@RequestParam(defaultValue = "1") int page_no,
                                @RequestParam(defaultValue = "15") int page_size) {
        ModelAndView modelAndView = new ModelAndView("/admin/card_old");
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        Admin admin = adminService.findAdminByName(authentication.getName());
        modelAndView.addObject("admin", admin);
        Page<Card> cards = cardService.getAllOldCard(page_size, page_no - 1);
        List<Card> cardList = cards.getContent();
        modelAndView.addObject("card_total", cards.getTotalElements());
        modelAndView.addObject("page_size", page_size);
        modelAndView.addObject("page_no", page_no);
        modelAndView.addObject("cards", cardList);
        return modelAndView;
    }

    @PostMapping("/8088/import_cards")
    public String importCards(@RequestParam MultipartFile file, @RequestParam int day) throws IOException {
        if (!file.isEmpty()) {
            List<Card> cards = new ArrayList<>();
            InputStream inputStream = file.getInputStream();
            LineIterator lineIterator = IOUtils.lineIterator(inputStream, "utf-8");
            while (lineIterator.hasNext()) {
                Card card = new Card(lineIterator.next().trim());
                card.setDay(day);
                cards.add(card);
            }
            cardService.importCards(cards);
        }
        return "redirect:/8088/card_new";
    }

    @PostMapping("/8088/delete_cards")
    @ResponseBody
    public String deleteCards(@RequestParam String data) {
        JSONArray array = JSONArray.parseArray(data);
        ArrayList<String> list = new ArrayList<>();
        for (Object anArray : array) {
            String item = (String) anArray;
            list.add(item);
        }
        String[] ids = (String[]) list.toArray(new String[0]);
        return String.valueOf(cardService.deleteCardById(ids));
    }

    @RequestMapping("/8088/user_log")
    public ModelAndView userLog(
            @RequestParam String nick,
            @RequestParam(defaultValue = "1") int page_no,
            @RequestParam(defaultValue = "15") int page_size) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        Admin admin = adminService.findAdminByName(authentication.getName());
        ModelAndView modelAndView = new ModelAndView("/admin/user_log");
        modelAndView.addObject("admin", admin);
        User user = userService.getUserByNick(nick);
        modelAndView.addObject("nick", nick);
        Page<Log> logPage = logService.getLogsByUser(user, page_no - 1, page_size);
        modelAndView.addObject("log_total", logPage.getTotalElements());
        modelAndView.addObject("page_size", page_size);
        modelAndView.addObject("page_no", page_no);
        List<Log> logs = logPage.getContent();
        modelAndView.addObject("logs", logs);
        return modelAndView;
    }

    @RequestMapping("/8088/password")
    public ModelAndView password() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        Admin admin = adminService.findAdminByName(authentication.getName());
        ModelAndView modelAndView = new ModelAndView("/admin/password");
        modelAndView.addObject("admin", admin);
        return modelAndView;
    }

    @PostMapping("/8088/do_password_mod")
    public String doPasswordMod(RedirectAttributes attributes,
                                @RequestParam String oldPassword,
                                @RequestParam String newPassword) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        Admin admin = adminService.findAdminByName(authentication.getName());
        if (passwordEncoder.matches(oldPassword, admin.getPassword())) {
            admin.setPassword(passwordEncoder.encode(newPassword));
            if (adminService.saveAdmin(admin)) {
                attributes.addFlashAttribute("info", new PageInfo("success", "密码修改成功"));
            } else {
                attributes.addFlashAttribute("info", new PageInfo("error", "密码修改失败"));
            }
        } else {
            attributes.addFlashAttribute("info", new PageInfo("error", "旧密码输入不正确"));
        }
        return "redirect:/8088/password";
    }
}
