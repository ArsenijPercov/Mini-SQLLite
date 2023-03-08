describe 'database' do
    def run_script(commands)
      raw_output = nil
      IO.popen("./main", "r+") do |pipe|
        commands.each do |command|
          pipe.puts command
        end
  
        pipe.close_write
  
        # Read entire output
        raw_output = pipe.gets(nil)
      end
      raw_output.split("\n")
    end
  
    it 'inserts and retrieves a row' do
      result = run_script([
        "insert 1 user1 person1@example.com",
        "select",
        ".exit",
      ])
      expect(result).to match_array([
        "db > Executed insert 1 user1 person1@example.com.",
        "db > {id:1, email:person1@example.com, user:user1 }",
        "Executed select.",
        "db > ",
      ])
    end

    it 'inserts more rows than we support and gets error' do
        script = (1..1401).map do |i|
            "insert #{i} user#{i} person#{i}@example.com"
        end
        script << ".exit"
        result = run_script([
            script
        ])
        expect(result[-2]).to eq('db > Cannot insert new data. Table is full.')
    end

    it 'allows insertiing user with max length' do
        user = 'a'*32
        result = run_script([
            "insert 1 #{user} person1@example.com",
            "select",
            ".exit"
        ])
        expect(result).to match_array([
            "db > Executed insert 1 #{user} person1@example.com.",
            "db > {id:1, email:person1@example.com, user:#{user} }",
            "Executed select.",
            "db > ",
          ])
    end
    it 'allows insertiing email with max length' do
        email = 'a'*255
        result = run_script([
            "insert 1 user1 #{email}",
            "select",
            ".exit"
        ])
        expect(result).to match_array([
            "db > Executed insert 1 user1 #{email}.",
            "db > {id:1, email:#{email}, user:user1 }",
            "Executed select.",
            "db > ",
          ])
    end
    it 'allows inserting email and user with max length' do
        email = 'a'*255
        user = 'a'*32
        result = run_script([
            "insert 1 #{user} #{email}",
            "select",
            ".exit"
        ])
        expect(result).to match_array([
            "db > Executed insert 1 #{user} #{email}.",
            "db > {id:1, email:#{email}, user:#{user} }",
            "Executed select.",
            "db > ",
          ])
    end
    it 'prints error when user exceeds max length' do
        email = 'a'
        user = 'a'*40
        result = run_script([
            "insert 1 #{user} #{email}",
            "select",
            ".exit"
        ])
        expect(result).to match_array([
            "db > Executed insert 1 #{user} #{email}.",
            "db > {id:1, email:#{email}, user:#{user} }",
            "Executed select.",
            "db > ",
          ])
    end
    it 'prints error when email exceeds max length' do
        email = 'a'*300
        user = 'a'
        result = run_script([
            "insert 1 #{user} #{email}",
            "select",
            ".exit"
        ])
        expect(result).to match_array([
            "Failed to parse query. The fields exceeded maximum length.",
            "db > {id:1, email:#{email}, user:#{user} }",
            "Executed select.",
            "db > ",
          ])
    end

    it 'prints error when user exceeds max length' do
        email = 'a'
        user = 'a'*300
        result = run_script([
            "insert 1 #{user} #{email}",
            "select",
            ".exit"
        ])
        expect(result).to match_array([
            "Failed to parse query. The fields exceeded maximum length.",
            "Executed select.",
            "db > ",
          ])
    end


    it 'prints error when too many arguments are passed' do
        email = 'a'
        user = 'a'*30
        result = run_script([
            "insert 1 #{user} #{email} 123213",
            "select",
            ".exit"
        ])
        expect(result).to match_array([
            "Failed to parse query. Too many fields were provided.",
            "Executed select.",
            "db > ",
          ])
    end

    it 'doesnt allow id to be a string' do

        result = run_script([
            "insert aaa user1 email@c.c",
            "select",
            ".exit"
        ])
        #TODO not sure what would be the correct response here
        expect(result).to match_array([ 
            "db > Failed to parse arguments for query insert aaa user1 email@c.c",
            "db > Executed select.",
            "db > ",
          ])
    end

    it 'allows to select on empty table' do

        result = run_script([
            "select",
            ".exit"
        ])
        #TODO not sure what would be the correct response here
        expect(result).to match_array([ 
            "db > Executed select.",
            "db > ",
          ])
    end
end