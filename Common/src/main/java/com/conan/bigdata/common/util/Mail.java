package com.conan.bigdata.common.util;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;

/**
 * Created by Administrator on 2018/7/25.
 */
public class Mail {
    private static final String SERVER = "smtp.exmail.qq.com";
    private static final String ACCOUNT = "big.data@puscene.com";
    private static final String PASSWORD = "AW#4esz";
    private static final String PORT = "465";

    private static SimpleEmail email = null;

    public static SimpleEmail getInstance() {
        if (email == null) {
            init();
        }
        return email;
    }

    private static void init() {
        email = new SimpleEmail();
        email.setHostName(SERVER);
        email.setAuthenticator(new DefaultAuthenticator(ACCOUNT, PASSWORD));
        email.setSslSmtpPort(PORT);
        email.setSSLOnConnect(true);
        try {
            email.setFrom(ACCOUNT);
        } catch (EmailException e) {
            e.printStackTrace();
        }
    }

    public static void sendEmail(String subject,String msg) {
        SimpleEmail email = getInstance();
        try {
            email.setSubject(subject);
            email.setMsg(msg);
            email.addTo("liu.feiqiang@puscene.com");
            email.send();
        } catch (EmailException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String subject = "自定义邮件";
        String msg = "这是自定义发送邮件";
        Mail.sendEmail(subject,msg);
    }
}