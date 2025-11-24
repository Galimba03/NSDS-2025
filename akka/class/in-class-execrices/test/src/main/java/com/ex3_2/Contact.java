package com.ex3_2;

public class Contact {

    private final String name;
    private final String email;

    public Contact() {
        this.name = "";
        this.email = "";
    }

    public Contact(String name, String email) {
        this.name = name;
        this.email = email;
    }

    public String getName() {
        return name;
    }

    public String getEmail() {
        return email;
    }
}