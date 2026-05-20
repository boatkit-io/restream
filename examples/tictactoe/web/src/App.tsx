import { ReStreamSocket } from '@boatkit-io/restream';
import './App.css'
import BoardStore from './stores/BoardStore'

import SocketIoClient from 'socket.io-client';
import { withResubAutoSubscriptions } from '@boatkit-io/resub';
import { PlaceTokenRequest } from './restream/PackageMain';

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

// eslint-disable-next-line react-refresh/only-export-components
function App() {
  const board = BoardStore.getBoard();
  const xTurn = BoardStore.getXTurn();
  const nextToken = xTurn ? 'X' : 'O';

  return (
    <>
      <h1>Tic Tac Toe</h1>
      <h2>Current Player: {nextToken}</h2>
      <div className="board">
        <table align="center">
          <tbody>
            {board.map((row, rowIndex) => (
              <tr key={rowIndex}>
                {row.map((cell, cellIndex) => (
                  <td key={cellIndex} onClick={async () => { try { await rss.sendRPC(PlaceTokenRequest.fromValues(cellIndex, rowIndex)); } catch (error) { alert(error); } }}>{cell || ' '}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}

export default withResubAutoSubscriptions(App)
