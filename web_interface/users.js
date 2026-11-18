// Lista de usuários válidos
// Esta lista deve ser mantida sincronizada com o arquivo user.txt
// Para adicionar/remover usuários, edite esta lista e o arquivo user.txt

const USERS_ACCESS = [
    { username: 'lsouza', is_admin: true },
    { username: 'estg.lsouza', is_admin: true },
    { username: 'usertest', is_admin: false }
];

const USER_MAP = USERS_ACCESS.reduce((acc, user) => {
    acc[user.username] = { is_admin: !!user.is_admin };
    return acc;
}, {});

// Função para verificar se um usuário é válido
function isValidUser(username) {
    return Object.prototype.hasOwnProperty.call(USER_MAP, username);
}

// Função para obter todos os usuários válidos
function getAllValidUsers() {
    return Object.keys(USER_MAP);
}

function isAdminUser(username) {
    return !!(USER_MAP[username] && USER_MAP[username].is_admin);
}

// Exportar funções para uso global
if (typeof window !== 'undefined') {
    window.isValidUser = isValidUser;
    window.getAllValidUsers = getAllValidUsers;
    window.isAdminUser = isAdminUser;
}
