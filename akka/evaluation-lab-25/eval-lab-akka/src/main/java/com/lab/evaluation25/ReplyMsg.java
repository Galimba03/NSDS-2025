package com.lab.evaluation25;

public class ReplyMsg extends GenericReplyMsg {

    private String email = null;

    public ReplyMsg(String email) {
        this.email = email;
    }

    public String getEmail() {
        return email;
    }

}
