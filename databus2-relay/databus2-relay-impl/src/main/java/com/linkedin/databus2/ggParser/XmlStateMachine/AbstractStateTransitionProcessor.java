package com.linkedin.databus2.ggParser.XmlStateMachine;


import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import com.linkedin.databus2.core.DatabusException;


public abstract class AbstractStateTransitionProcessor implements StateProcessor
{

  private static Map<TransitionElement, HashSet<TransitionElement>> _transitionMapping;
  protected STATETYPE _currentStateType;
  protected String _currentState;

  //States
  public static final String COLUMNSTATE = "column";
  public static final String DBUPDATE = "dbupdate";
  public static final String TRANSACTION = "transaction";
  public static final String COLUMNSSTATE = "columns";
  public static final String TOKENS = "tokens";
  public static final String ROOT = "root";
  public static final String TOKENSTATE = "token";
  public static final String TOKENSSTATE = "tokens";

  static
  {
    _transitionMapping = new HashMap<TransitionElement, HashSet<TransitionElement>>();

    TransitionElement startState = null, nextState1 = null, nextState2 = null, nextState3 = null;

    //transaction start tag mapping, this indicates a state transition from <transaction> -> <dbupdate> is valid.
    startState = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, TRANSACTION);
    nextState1 = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, DBUPDATE);
    addToTransitionMap(startState, nextState1);

    //Transaction end tag mapping
    startState = new TransitionElement(StateProcessor.STATETYPE.ENDELEMENT, TRANSACTION);
    nextState1 = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, TRANSACTION);
    nextState2 = new TransitionElement(StateProcessor.STATETYPE.ENDELEMENT, ROOT);
    addToTransitionMap(startState, nextState1, nextState2);

    //Dbupdate start tag mapping
    startState = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, DBUPDATE);
    nextState1 = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, COLUMNSSTATE);
    nextState2 = new TransitionElement(StateProcessor.STATETYPE.ENDELEMENT, DBUPDATE); //This state is possible when we skip the current dbUpdate
    nextState3 = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, TOKENSSTATE); //This state is possible when we skip the current dbUpdate
    addToTransitionMap(startState, nextState1, nextState2, nextState3);

    //Dbupdate end tag mapping
    startState = new TransitionElement(StateProcessor.STATETYPE.ENDELEMENT, DBUPDATE);
    nextState1 = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, DBUPDATE);
    nextState2 = new TransitionElement(StateProcessor.STATETYPE.ENDELEMENT, TRANSACTION);
    addToTransitionMap(startState, nextState1, nextState2);

    //Columns start tag mapping
    startState = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, COLUMNSSTATE);
    nextState1 = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, COLUMNSTATE);
    addToTransitionMap(startState, nextState1);

    //columns end tag mapping
    startState = new TransitionElement(StateProcessor.STATETYPE.ENDELEMENT, COLUMNSSTATE);
    nextState1 = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, TOKENSSTATE);
    addToTransitionMap(startState, nextState1);

    //Tokens start tag mapping
    startState = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, TOKENSSTATE);
    nextState1 = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, TOKENSTATE);
    addToTransitionMap(startState, nextState1);

    //Tokens end tag mapping
    startState = new TransitionElement(StateProcessor.STATETYPE.ENDELEMENT, TOKENSSTATE);
    nextState1 = new TransitionElement(StateProcessor.STATETYPE.ENDELEMENT, DBUPDATE);
    addToTransitionMap(startState, nextState1);

    //Token start tag mapping
    startState = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, TOKENSTATE);
    nextState1 = new TransitionElement(StateProcessor.STATETYPE.ENDELEMENT,TOKENSTATE);
    addToTransitionMap(startState, nextState1);

    //Token end tag mapping
    startState = new TransitionElement(StateProcessor.STATETYPE.ENDELEMENT, TOKENSTATE);
    nextState1 = new TransitionElement(StateProcessor.STATETYPE.ENDELEMENT, TOKENSSTATE);
    nextState2 = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, TOKENSTATE);
    addToTransitionMap(startState, nextState1, nextState2);

    //Column start tag mapping
    startState = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, COLUMNSTATE);
    nextState1 = new TransitionElement(StateProcessor.STATETYPE.ENDELEMENT, COLUMNSTATE);
    addToTransitionMap(startState, nextState1);

    //Column end tag mapping
    startState = new TransitionElement(StateProcessor.STATETYPE.ENDELEMENT, COLUMNSTATE);
    nextState1 = new TransitionElement(StateProcessor.STATETYPE.ENDELEMENT, COLUMNSSTATE);
    nextState2 = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, COLUMNSTATE);
    addToTransitionMap(startState, nextState1, nextState2);

    //Root start tag mapping
    startState = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, ROOT);
    nextState1 = new TransitionElement(StateProcessor.STATETYPE.STARTELEMENT, TRANSACTION);
    addToTransitionMap(startState, nextState1);

    //Root end tag mapping
    //After this the xml processing is done.

    _transitionMapping = Collections.unmodifiableMap(_transitionMapping);
  }

  private static void addToTransitionMap(TransitionElement key, TransitionElement ... values)
  {
    HashSet<TransitionElement> possibleStates = new HashSet<TransitionElement>();

    for(int i=0; i < values.length; i++)
      possibleStates.add(values[i]);

    _transitionMapping.put(key,possibleStates);
  }

  /**
   * Holds the transition information
   * For e.g., _statetype = STARTELEMENT and _currentTransitionState = "DbUpdate" indicates => <dbupdate>
   */
  private static class TransitionElement
  {
    //The start
    StateProcessor.STATETYPE _statetype;
    String _currentTransitionState;

    TransitionElement(StateProcessor.STATETYPE stateType, String currentTransitionState)
    {
      _statetype = stateType;
      _currentTransitionState = currentTransitionState;
    }

    @Override
    public String toString()
    {
      return  _currentTransitionState + "(" + _statetype + ")";
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TransitionElement that = (TransitionElement) o;

      if (_currentTransitionState != null ? !_currentTransitionState.equals(that._currentTransitionState) : that._currentTransitionState != null)
        return false;
      if (_statetype != that._statetype) return false;

      return true;
    }

    @Override
    public int hashCode()
    {
      int result = _statetype != null ? _statetype.hashCode() : 0;
      result = 31 * result + (_currentTransitionState != null ? _currentTransitionState.hashCode() : 0);
      return result;
    }
  }

  /**
   * In the constructor we define what are the possible state transitions.
   * @param currentStateType
   * @param currentState
   */
  public AbstractStateTransitionProcessor(StateProcessor.STATETYPE currentStateType, String currentState){

    _currentState = currentState;
    _currentStateType = currentStateType;
  }


  @Override
  public void processElement(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws Exception
  {
    if(!xmlStreamReader.getLocalName().equals(_currentState))
      throw new DatabusException("Unexpected state, expected " + _currentState + ", found: " + xmlStreamReader.getLocalName());

    if(xmlStreamReader.isEndElement())
    {
      onEndElement(stateMachine, xmlStreamReader);
    }
    else
      onStartElement(stateMachine, xmlStreamReader);
  }


  @Override
  public void setNextStateProcessor(StateMachine stateMachine, XMLStreamReader xmlStreamReader)
      throws DatabusException, XMLStreamException
  {

    String nextState = xmlStreamReader.getLocalName();
    StateProcessor.STATETYPE nextStateType = null;

    if(xmlStreamReader.isEndElement())
      nextStateType = StateProcessor.STATETYPE.ENDELEMENT;
    else if(xmlStreamReader.isStartElement())
      nextStateType = StateProcessor.STATETYPE.STARTELEMENT;
    else
      throw new DatabusException("Neither Start element or End element! cannot transition states");

    validateAndSetStateProcessor(stateMachine, _currentState, _currentStateType, nextState, nextStateType);
  }

  private void validateAndSetStateProcessor(StateMachine stateMachine,
                                    String currentState,
                                    StateProcessor.STATETYPE currentStateType,
                                    String nextState,
                                    StateProcessor.STATETYPE nextStateType)
      throws DatabusException
  {
    validateNextTransition(stateMachine, currentState, currentStateType, nextState, nextStateType);
    //Set the next state
    StateProcessor stateProcessor = fetchNextState(stateMachine, nextState);
    stateMachine.setProcessState(stateProcessor);
  }

  private StateProcessor fetchNextState(StateMachine stateMachine, String nextState)
      throws DatabusException
  {
    if(nextState.equals(TransactionState.TRANSACTION))
      return stateMachine.transactionState;
    else if(nextState.equals(DbUpdateState.DBUPDATE))
      return stateMachine.dbUpdateState;
    else if(nextState.equals(ColumnsState.COLUMNSSTATE))
      return stateMachine.columnsState;
    else if(nextState.equals(ColumnsState.COLUMNSTATE))
      return stateMachine.columnState;
    else if(nextState.equals(TokensState.TOKENSSTATE))
      return stateMachine.tokensState;
    else if(nextState.equals(TokenState.TOKENSTATE))
      return stateMachine.tokenState;
    else if(nextState.equals(RootState.ROOT))
      return stateMachine.rootState;
    else
      throw new DatabusException("Unknown state (" + nextState +")! unable to find a corresponding state processor");

  }

  private void validateNextTransition(StateMachine stateMachine,
                                      String currentState,
                                      StateProcessor.STATETYPE currentStateType,
                                      String nextState,
                                      StateProcessor.STATETYPE nextStateType)
      throws DatabusException
  {
    TransitionElement startTransition = new TransitionElement(currentStateType, currentState);
    TransitionElement endTransition = new TransitionElement(nextStateType, nextState);

    HashSet<TransitionElement> expectedTranstitions = _transitionMapping.get(startTransition);

    if(expectedTranstitions == null)
      throw new DatabusException("No mapping found for this particular state "+ startTransition +". The state machine does not know the expected transitions");

    if(!expectedTranstitions.contains(endTransition))
    {
      throw new DatabusException("The current state is : "+ startTransition  +" the expected state was: " + expectedTranstitions + ". The next state found was: " + endTransition);
    }

   }
}
