package kite.datasource;

import java.net.http.HttpRequest;

// todo: add context
// steal API ideas from: https://github.com/Umbyr93/EasyHttpClient
public record Request(HttpRequest http) {}
