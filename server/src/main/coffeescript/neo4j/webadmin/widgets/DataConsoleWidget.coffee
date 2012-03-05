
define ['ribcage/View','ribcage/Model'], (View, Model) ->

  class LineState
    SINGLE : 0
    MULTI  : 1

  class DataConsoleView extends View
    
    

  class DataConsoleWidget extends Model
    
    defaults :
      lineState : LineState.SINGLE

    useMultilineMode : -> 
      @set 'lineState' : LineState.MULTI
