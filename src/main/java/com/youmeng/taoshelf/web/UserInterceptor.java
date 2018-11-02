package com.youmeng.taoshelf.web;

import com.youmeng.taoshelf.entity.User;
import com.youmeng.taoshelf.service.UserService;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Date;

@Component
public class UserInterceptor implements HandlerInterceptor {

    @Resource
    private UserService userService;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
//        request.getSession().setAttribute("nick", "yang");
        String[] s = new String[]{
                "/taoshelf/submit_card_key",
                "/taoshelf/recharge",
                "/taoshelf/pick_up",
                "/error",
                "/taoshelf/static/"
        };
        for (String value : s) {
            if (request.getRequestURI().contains(value)) {
                return true;
            }
        }
        if (request.getRequestURI().contains("/8088/")) {
            return true;
        } else {
            String contextPath = request.getContextPath();
            String nick = (String) request.getSession().getAttribute("nick");
            if (nick == null) {
                response.sendRedirect("https://oauth.taobao.com/authorize?response_type=token&client_id=12322527&redirect_uri=http://ruyisoft.com/redirect/smart_zhanggui.php");
                return false;
            } else {
                User user = userService.getUserByNick(nick);
                if (user.getEndTime().before(new Date())) {
                    response.sendRedirect(contextPath + "/recharge");
                    return false;
                } else {
                    return true;
                }
            }
        }
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) throws Exception {

    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {

    }

}

