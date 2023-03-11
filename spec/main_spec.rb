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
        "db > Executed.",
        "db > {id:1, email:person1@example.com, user:user1 }",
        "Executed.",
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
            "db > Executed.",
            "db > {id:1, email:person1@example.com, user:#{user} }",
            "Executed.",
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
            "db > Executed.",
            "db > {id:1, email:#{email}, user:user1 }",
            "Executed.",
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
            "db > Executed.",
            "db > {id:1, email:#{email}, user:#{user} }",
            "Executed.",
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
            "db > Executed.",
            "db > Failed to parse query. The fields exceeded maximum length.",
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
            "db > Failed to parse query. The fields exceeded maximum length.",  
            "db > Executed.", 
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
            "db > ",
            "db > Executed.", 
            "db > Failed to parse query. The fields exceeded maximum length."
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
            "db > ",
            "db > Executed.",
            "db > Failed to parse query. Too many fields were provided."
          ])
    end

    it 'allows id to be a string, and defaults it to 0' do

        result = run_script([
            "insert aaa user1 email@c.c",
            "select",
            ".exit"
        ])
        expect(result).to match_array([ 
            "db > {id:0, email:email@c.c, user:user1 }",
            "db > Executed.",
            "db > ",
            "Executed."
          ])
    end

    it 'prints error when id is negative' do

        result = run_script([
            "insert -1 user1 email@c.c",
            "select",
            ".exit"
        ])
        expect(result).to match_array([ 
            "db > Executed.",
            "db > Failed to parse the query. It contains a negative id.",
            "db > ",
          ])
    end

    it 'allows to select on empty table' do

        result = run_script([
            "select",
            ".exit"
        ])
        expect(result).to match_array([ 
            "db > Executed.",
            "db > ",
          ])
    end
end