import React, { useEffect, useRef } from 'react';
import L from 'leaflet';

interface ClientTrackingMapProps {
    orderId: string;
    stage: string;
    originLat?: number;
    originLng?: number;
    destLat?: number;
    destLng?: number;
    currentLat?: number;
    currentLng?: number;
    assignedDriver?: string;
    assignedVehicle?: string;
}

export const ClientTrackingMap: React.FC<ClientTrackingMapProps> = ({
                                                                        stage,
                                                                        originLat = 6.9388,
                                                                        originLng = 79.8745,
                                                                        destLat = 6.9016,
                                                                        destLng = 79.8654,
                                                                        currentLat = 6.9388,
                                                                        currentLng = 79.8745,
                                                                        assignedDriver,
                                                                        assignedVehicle,
                                                                    }) => {
    const containerRef = useRef<HTMLDivElement | null>(null);
    const mapRef = useRef<L.Map | null>(null);
    const vanMarkerRef = useRef<L.CircleMarker | null>(null);
    const progressLineRef = useRef<L.Polyline | null>(null);

    useEffect(() => {
        if (!containerRef.current || mapRef.current) return;

        const map = L.map(containerRef.current, {
            center: [(originLat + destLat) / 2, (originLng + destLng) / 2],
            zoom: 13,
            zoomControl: false,
        });

        L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            attribution: '&copy; OpenStreetMap',
        }).addTo(map);

        L.circleMarker([originLat, originLng], {
            radius: 6,
            color: '#38bdf8',
            fillColor: '#0284c7',
            fillOpacity: 1,
        })
            .bindPopup('🏭 SwiftLogistics Warehouse')
            .addTo(map);

        L.circleMarker([destLat, destLng], {
            radius: 6,
            color: '#f43f5e',
            fillColor: '#e11d48',
            fillOpacity: 1,
        })
            .bindPopup('📍 Customer Delivery Point')
            .addTo(map);

        L.polyline(
            [
                [originLat, originLng],
                [destLat, destLng],
            ],
            { color: '#334155', weight: 3, dashArray: '6, 6' }
        ).addTo(map);

        progressLineRef.current = L.polyline(
            [
                [originLat, originLng],
                [currentLat, currentLng],
            ],
            { color: '#38bdf8', weight: 4 }
        ).addTo(map);

        vanMarkerRef.current = L.circleMarker([currentLat, currentLng], {
            radius: 8,
            color: '#ffffff',
            weight: 2,
            fillColor: '#10b981',
            fillOpacity: 1,
        }).addTo(map);

        mapRef.current = map;

        return () => {
            map.remove();
            mapRef.current = null;
        };
    }, [originLat, originLng, destLat, destLng]);

    // Smoothly update van marker whenever Notification Service sends new currentLat/currentLng
    useEffect(() => {
        if (!vanMarkerRef.current || !progressLineRef.current) return;
        vanMarkerRef.current.setLatLng([currentLat, currentLng]);
        progressLineRef.current.setLatLngs([
            [originLat, originLng],
            [currentLat, currentLng],
        ]);
    }, [currentLat, currentLng, originLat, originLng]);

    return (
        <div className="mt-4 pt-3 border-t border-slate-800/80 space-y-2" onClick={(e) => e.stopPropagation()}>
            <div className="flex items-center justify-between text-xs text-slate-400">
        <span>
          🧑‍✈️ <strong className="text-slate-200">{assignedDriver || 'Driver Assigned'}</strong> •{' '}
            <span className="text-sky-400">{assignedVehicle}</span>
        </span>
                <span className="font-mono text-[11px] text-emerald-400">
          {stage === 'IN_TRANSIT' ? '📡 LIVE GPS TELEMETRY' : `STATUS: ${stage}`}
        </span>
            </div>
            <div ref={containerRef} className="w-full h-44 rounded-xl overflow-hidden border border-slate-800" />
        </div>
    );
};