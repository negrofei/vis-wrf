' ABRIR_VIS_WRF.vbs - Visor WRF Argentina
' Ejecuta el servidor Python en segundo plano y abre el navegador.

Option Explicit
Dim shell, fso, scriptDir, pythonExe, serverPath, browserUrl

Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
serverPath = fso.BuildPath(scriptDir, "server.py")
browserUrl = "http://127.0.0.1:8765"

' Verificar que existe server.py
If Not fso.FileExists(serverPath) Then
    MsgBox "No se encontró server.py en:" & vbCrLf & scriptDir, vbCritical, "Visor WRF"
    WScript.Quit 1
End If

' Buscar Python
pythonExe = "python"

' Lanzar servidor en segundo plano (sin ventana)
shell.Run "cmd /c cd /d """ & scriptDir & """ && " & pythonExe & " server.py", 0, False

' Esperar a que el servidor levante
WScript.Sleep 1500

' Abrir navegador
shell.Run browserUrl, 1, False
