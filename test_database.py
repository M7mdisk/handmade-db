import subprocess


def run_commands(commands):
    proc_input = "\n".join(commands)
    process = subprocess.run(
        ["./main"],
        input=proc_input,
        encoding="utf-8",
        stdout=subprocess.PIPE,
    )
    return process.stdout.split("\n")


class TestDatabase:
    def test_db(self):
        res = run_commands(
            [
                "insert 1 user1 person1@example.com",
                "select",
                ".exit ",
            ]
        )
        assert res == ["db> ", "db> 1 | person1@example.com | user1", "db> "]

    def test_many_inserts(self):
        count = 1000
        cmds = [f"insert {i} user{i} p{i}@gmail.com" for i in range(1, count)]
        res = run_commands(
            cmds
            + [
                ".exit ",
            ]
        )
        assert res == ["db> "] * count

    def test_full_table(self):
        count = 102
        cmds = [f"insert {i} user{i} p{i}@gmail.com" for i in range(1, count)]
        res = run_commands(
            cmds
            + [
                ".exit ",
            ]
        )
        print(res)
        print(len(res))
        assert res == ["db> "] * count
