package com.youmeng.taoshelf.web;

import com.taobao.api.ApiException;
import com.taobao.api.TaobaoClient;
import com.taobao.api.request.AppstoreSubscribeGetRequest;
import com.taobao.api.response.AppstoreSubscribeGetResponse;
import com.youmeng.taoshelf.entity.PageInfo;
import com.youmeng.taoshelf.entity.User;
import com.youmeng.taoshelf.service.CardService;
import com.youmeng.taoshelf.service.UserService;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.util.regex.Pattern;

@Controller
public class UserController {

    @Resource(name = "client1")
    private TaobaoClient client1;

    @Resource(name = "client2")
    private TaobaoClient client2;

    @Resource
    private UserService userService;

    @Resource
    private CardService cardService;

    //回调接收地址
    @RequestMapping("/pick_up")
    public String pickUp(HttpSession session,
                         @RequestParam(defaultValue = "") String nick,
                         @RequestParam(defaultValue = "", name = "session") String token,
                         @RequestParam String appkey) throws ApiException {
        //检验参数
        Pattern pattern1 = Pattern.compile("^(?!_)(?!.*?_$)[a-zA-Z0-9_\\u4e00-\\u9fa5]+$");
        if (!pattern1.matcher(nick).matches()) {
            return "redirect:/error";
        }
        Pattern pattern2 = Pattern.compile(".*[`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？\\\\]+.*");
        if (pattern2.matcher(token).matches()) {
            return "redirect:/error";
        }
        if (appkey.equals("12322527")) {
            if (nick.equals("") || token.equals("")) {
                return "redirect:https://tb.cn/VdsUTMw";
            }
        } else if (appkey.equals("12402170")) {
            if (nick.equals("") || token.equals("")) {
                return "redirect:https://tb.cn/QKThZNw";
            }
        }
        User user = userService.getUserByNick(nick);
        if (user == null) {
            user = new User(nick);
        }
        AppstoreSubscribeGetRequest request = new AppstoreSubscribeGetRequest();
        request.setNick(nick);
        AppstoreSubscribeGetResponse response;
        if (appkey.equals("12322527")) {
            response = client1.execute(request);
            if (response.getUserSubscribe() == null) {
                return "redirect:https://tb.cn/VdsUTMw";
            } else {
                user.setEndDate1(response.getUserSubscribe().getEndDate());
                user.setSessionKey1(token);
                userService.saveUser(user);
                return "redirect:http://container.api.taobao.com/container?appkey=12402170&redirect_uri=http://taoba.lingyunwl.net/wlmdetail/rederect.php&state=youmeng";
            }
        } else if (appkey.equals("12402170")) {
            response = client2.execute(request);
            if (response.getUserSubscribe() == null) {
                return "redirect:https://tb.cn/QKThZNw";
            } else {
                user.setEndDate2(response.getUserSubscribe().getEndDate());
                user.setSessionKey2(token);
                userService.saveUser(user);
                session.setAttribute("nick", nick);
            }
        }
        return "redirect:/home";
    }

    //主页面
    @RequestMapping("/home")
    public ModelAndView home(HttpSession session) {
        ModelAndView modelAndView = new ModelAndView("/user/home");
        User user = userService.getUserByNick((String) session.getAttribute("nick"));
        modelAndView.addObject("user", user);
        return modelAndView;
    }

    //充值页面
    @RequestMapping("recharge")
    public ModelAndView recharge(HttpSession session) {
        ModelAndView modelAndView = new ModelAndView("/user/recharge");
        User user = userService.getUserByNick((String) session.getAttribute("nick"));
        modelAndView.addObject("user", user);
        return modelAndView;
    }

    //充值
    @PostMapping("submit_card_key")
    public String submitCardKey(HttpSession session, RedirectAttributes attributes, @RequestParam String key) {
        User user = userService.getUserByNick((String) session.getAttribute("nick"));
        if (cardService.useCard(key.trim(), user)) {
            attributes.addFlashAttribute("info", new PageInfo("success", "充值成功"));
            return "redirect:/recharge";
        } else {
            attributes.addFlashAttribute("info", new PageInfo("error", "无效的卡密"));
            return "redirect:/recharge";
        }
    }

}
