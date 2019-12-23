#include "cli.h"

#define SUCCESS 0
#define MODE 0  // 0 = TEST MODE
// 1 = INTERACTIVE MODE
// 3 = TEST + INTERACTIVE MODE

CLI *cli;

void exec(const std::string &command, bool equal = true) {
    std::cout << ">>> " << command << std::endl;

    if (equal)
        assert (cli->process(command) == SUCCESS);
    else
        assert (cli->process(command) != SUCCESS);
}

// Block Nested Loop Join
void Test08() {
    std::cout << "*********** CLI Test08 begins ******************" << std::endl;

    std::string command;

    exec("create table employee EmpName = varchar(30), Age = int, Height = real, Salary = int");

    exec("create table ages Age = int, Explanation = varchar(50)");

    exec("create table salary Salary = int, Explanation = varchar(50)");

    exec("load employee employee_5");

    exec("load ages ages_90");

    exec("load salary salary_5");

    exec("SELECT BNLJOIN employee, ages WHERE Age = Age PAGES(10)");

    exec("SELECT BNLJOIN (BNLJOIN employee, salary WHERE Salary = Salary RECORDS(10)), ages WHERE Age = Age PAGES(10)");

    exec("SELECT BNLJOIN (BNLJOIN (BNLJOIN employee, employee WHERE EmpName = EmpName PAGES(10)), salary) WHERE Salary = Salary PAGES(10)), ages WHERE Age = Age PAGES(10)");

    exec(("drop table employee"));

    exec(("drop table ages"));

    exec(("drop table salary"));
}

int main() {

    cli = CLI::Instance();

    if (MODE == 0 || MODE == 3) {
        Test08(); // BNLJoin
    }
    if (MODE == 1 || MODE == 3) {
        cli->start();
    }

    return 0;
}
