function hello(who) {
    return "hello " + who;
}
    
greeting = spawn(hello,["world"]);
return *greeting;
