import { ReStreamSocket } from '@boatkit-io/restream';
import './App.css'
import BoardStore from './stores/BoardStore'

import SocketIoClient from 'socket.io-client';
import { withResubAutoSubscriptions } from 'resub';

const socket = SocketIoClient('http://localhost:8080', {
  path: '/socket',
  reconnection: true,
});

const rss = new ReStreamSocket(socket);

socket.on('connect', () => {
  // no auth, just send it
  rss.markAuthenticated();
});

socket.open();

function App() {
  const board = BoardStore.getBoard();
  const xTurn = BoardStore.getXTurn();
  
  return (
    <>
      <h1>Tic Tac Toe</h1>
      <div className="board">
        <table align="center">
        {board.map((row, rowIndex) => (
          <tr key={rowIndex}>
            {row.map((cell, cellIndex) => (
              <td key={cellIndex}>{cell || ' '}</td>
            ))}
          </tr>
        ))}
        </table>
      </div>
      <p>It is {xTurn ? 'X' : 'O'}'s turn</p>
    </>
  )
}

export default withResubAutoSubscriptions(App)
