import cv2
import sys
import os

def convert_to_custom_mjpeg(input_video_path, output_mjpeg_path):
    # Abre o vídeo original
    cap = cv2.VideoCapture(input_video_path)
    
    if not cap.isOpened():
        print(f"Erro: Não foi possível abrir o vídeo '{input_video_path}'")
        return

    print(f"Convertendo '{input_video_path}' para '{output_mjpeg_path}'...")

    with open(output_mjpeg_path, 'wb') as out_file:
        frame_count = 0
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            
            # Redimensionar se necessário (opcional, para deixar mais leve)
            frame = cv2.resize(frame, (480, 360))

            # Codifica o frame atual para formato JPEG
            # O segundo parâmetro [int(cv2.IMWRITE_JPEG_QUALITY), 50] define a qualidade (0-100)
            ret, jpeg_data = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 25])
            
            if not ret:
                continue
            
            data_bytes = jpeg_data.tobytes()
            size = len(data_bytes)
            
            # Cria o cabeçalho de 5 bytes (ex: 12345 -> "12345", 500 -> "00500")
            # Se o frame for maior que 99999 bytes, isso vai quebrar o formato original,
            # mas para vídeos leves (360p/480p) geralmente funciona bem.
            if size > 99999:
                print(f"Aviso: Frame {frame_count} excedeu 99999 bytes ({size}). O formato pode quebrar.")
            
            header = f"{size:05d}".encode('ascii')
            
            # Escreve Cabeçalho + Dados
            out_file.write(header)
            out_file.write(data_bytes)
            frame_count += 1
            
            if frame_count % 100 == 0:
                print(f"Processados {frame_count} frames...")
            
    print(f"Sucesso! {frame_count} frames salvos em '{output_mjpeg_path}'")
    cap.release()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python video_converter.py <video_entrada.mp4> <video_saida.Mjpeg>")
        print("Exemplo: python video_converter.py meu_video.mp4 videos/meu_filme.Mjpeg")
    else:
        convert_to_custom_mjpeg(sys.argv[1], sys.argv[2])
