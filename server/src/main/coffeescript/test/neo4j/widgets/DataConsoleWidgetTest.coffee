
define ["neo4j/webadmin/widgets/DataConsoleWidget"], (DataConsoleWidget) ->

  describe "DataConsoleWidget", ->
    dcw = new DataConsoleWidget()

    it "can be instantiated", ->  
      expect(dcw).toBeDefined()

    it "can be switched between multi line and single line mode", ->
      dcw.useMultilineMode()
